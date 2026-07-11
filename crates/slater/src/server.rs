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
use graph_format::ids::Value;
use graph_format::store::fs::FsObjectStore;
use graph_format::store::{join_key, ObjectStore};
use rayon::prelude::*;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
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
    /// Per-graph writable-layer writers, populated only when the delta layer is
    /// enabled (`config.delta.enabled`). Empty otherwise — the read-only server is
    /// exactly what it was. Each writer is bound to the generation it resolved its
    /// dense ids against (`DeltaWriter::core_uuid`).
    writers: HashMap<String, Arc<DeltaWriter>>,
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
        Self::open_all_with_store(store, master_key, true, None)
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
                )
                .with_context(|| format!("open graph {name}"))?;
                Ok((name, RwLock::new(Arc::new(gen))))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self {
            store,
            master_key: master_key.map(<[u8]>::to_vec),
            verify_integrity,
            graphs,
            acl_path: None,
            require_acl_stamp: false,
            range_index_cache_bytes,
            writers: HashMap::new(),
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
    fn writer(&self, name: &str) -> Option<Arc<DeltaWriter>> {
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
            Generation::open_with_store_opts_cached(
                self.store.as_ref(),
                name,
                self.master_key.as_deref(),
                self.verify_integrity,
                self.range_index_cache_bytes,
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
            bail!("a consolidation for '{name}' is already in progress");
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

        // Publish: pick up the freshly built generation (validated + PQ-pinned) and
        // swap the served slot to it.
        let new_uuid = self
            .swap_if_changed(name, vector_cache)
            .with_context(|| format!("swap in consolidated generation for '{name}'"))?
            .ok_or_else(|| {
                anyhow!("builder for '{name}' did not publish a new generation (current unchanged)")
            })?;

        // Retire: the delta now lives in the new core, so drop the consumed WAL
        // segments and re-bind the writer to the new generation (re-basing the
        // synthetic node/edge id spaces on the new core's node/edge counts). Any
        // post-freeze write is replayed onto the new core via `resolve_op` bound to the
        // freshly-swapped generation — a business key that was delta-born pre-freeze
        // re-resolves to its now-real dense id (Phase 4a).
        let new_gen = self.get(name).ok_or_else(|| {
            anyhow!("consolidated generation for '{name}' vanished before retire")
        })?;
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
            bail!("a consolidation or flush for '{name}' is already in progress");
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

        // Fold the frozen snapshot with any spilled L0 levels into ONE newest-wins memtable
        // (Phase 4c). The active memtable is newest; `frozen.l0` is newest-first — so the
        // `merge_levels` order is `[snapshot, l0[0]…l0[n]]`. Every level was resolved against
        // the same served core, so they share a `synthetic_base`; the merged memtable inherits
        // it (= `prior_node_total`), keeping the writer's Phase-3.2 band assertion true. The
        // no-L0 fast path flushes the snapshot directly (unchanged), so the merge — and its
        // clone of the folded state — is only paid when levels actually stacked.
        let merged: Option<Memtable> = if frozen.l0.is_empty() {
            None
        } else {
            let mut levels: Vec<&Memtable> = Vec::with_capacity(1 + frozen.l0.len());
            levels.push(frozen.snapshot.as_ref());
            for lvl in &frozen.l0 {
                match lvl.as_memtable() {
                    Some(m) => levels.push(m),
                    // Off-heap L0 stores a block image, not a memtable, so `merge_levels`
                    // cannot fold it (the `LevelRead` trait is lossy for a full rebuild).
                    // Resident L0 (the default) folds; off-heap flush is a later slice.
                    None => bail!(
                        "flush_to_segment over an off-heap L0 level is not yet supported \
                         (resident L0 folds; off-heap needs a memtable rebuild)"
                    ),
                }
            }
            Some(Memtable::merge_levels(&levels))
        };
        let flush_mem: &Memtable = merged.as_ref().unwrap_or_else(|| frozen.snapshot.as_ref());

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
            match crate::flush_segment::write_flush_segment(flush_mem, &inp) {
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

        // Swap the served generation to the new set (its stack now carries the segment).
        let new_uuid = self
            .swap_if_changed(name, vector_cache)
            .with_context(|| format!("swap in flushed set for '{name}'"))?
            .ok_or_else(|| {
                anyhow!("flush for '{name}' published a set but `current` was unchanged")
            })?;
        debug_assert_eq!(new_uuid, set_uuid);
        let new_gen = self
            .get(name)
            .ok_or_else(|| anyhow!("flushed set for '{name}' vanished before retire"))?;

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
            bail!("a consolidation or flush for '{name}' is already in progress");
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

        let new_uuid = self
            .swap_if_changed(name, vector_cache)
            .with_context(|| format!("swap in compacted set for '{name}'"))?
            .ok_or_else(|| {
                anyhow!("compaction for '{name}' published a set but `current` was unchanged")
            })?;
        debug_assert_eq!(new_uuid, set_uuid);
        let new_gen = self
            .get(name)
            .ok_or_else(|| anyhow!("compacted set for '{name}' vanished before rebind"))?;

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
                    bail!("a consolidation or flush for '{name}' is already in progress");
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
    /// delta into a core segment. Suppressed when `off_heap_l0` (that flush still bails).
    segment_flush_bytes: usize,
    /// Upper core-segment count that admits a T3 segment→segment compaction after a write
    /// (`config.delta.max_upper_segments`; 0 disables — Phase 5.3 policy, Phase 6 auto-fire).
    max_upper_segments: usize,
    /// Whether this server reads L0 off-heap (`config.delta.off_heap_l0`). Consulted only
    /// to suppress the T2 auto-flush (an off-heap flush is not yet supported — it bails).
    off_heap_l0: bool,
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
#[derive(Debug)]
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
    let mut graphs = Graphs::open_all_with_store(
        store,
        master_key.as_deref(),
        verify_integrity,
        Some(cfg.cache.range_index_cache_bytes),
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
        data_dir: PathBuf::from(cfg.data_dir()),
        builder_bin: cfg.delta.builder_bin.clone(),
        memtable_bytes: cfg.delta.memtable_bytes,
        l0_compaction_trigger: cfg.delta.l0_compaction_trigger,
        segment_flush_bytes: cfg.delta.segment_flush_bytes,
        max_upper_segments: cfg.delta.max_upper_segments,
        off_heap_l0: cfg.delta.off_heap_l0,
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
            let param_vals = params_to_vals(&params)?;
            // The writable layer is per-graph and off unless configured; when it is
            // on, a query may be a write. Parse synchronously so a syntax /
            // read-only error is classified cleanly.
            let writer = ctx.graphs.writer(&graph);
            let (columns, rows) = match &writer {
                Some(w) => {
                    let stmt = parser::parse_statement(&query)
                        .map_err(|e| Failure::from_query_error(&e))?;
                    // A `read` grant selected the graph; mutating it needs `write` too.
                    authorize_statement(&ctx.acl.snapshot(), &user, &graph, &stmt)?;
                    match stmt {
                        parser::ast::Statement::Write(stmt) => {
                            let out = execute_write(w, gen.as_ref(), &stmt, &param_vals)?;
                            maybe_maintain_delta(ctx, &graph, w).await;
                            out
                        }
                        parser::ast::Statement::Create(stmt) => {
                            let out = execute_create(w, gen.as_ref(), &stmt, &param_vals)?;
                            maybe_maintain_delta(ctx, &graph, w).await;
                            out
                        }
                        parser::ast::Statement::WriteEdge(stmt) => {
                            let out = execute_edge_write(w, gen.as_ref(), &stmt, &param_vals)?;
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
                    let ast = parser::parse(&query).map_err(|e| Failure::from_query_error(&e))?;
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
        ReadOverlay {
            delta: writer.delta_snapshot(),
            epoch: writer.epoch(),
        }
    } else {
        warn!(
            graph = %gen.graph(),
            "writable-layer delta resolved against a superseded generation — serving pure core"
        );
        ReadOverlay::empty()
    }
}

/// Evaluate a Phase 1c write's constant expression (a literal or a parameter) to a
/// storable [`Value`] against the query's parameters.
fn write_value(
    e: &parser::ast::Expr,
    params: &HashMap<String, Val>,
    what: &str,
) -> std::result::Result<Value, Failure> {
    use parser::ast::Expr;
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
    let map_err = |e: anyhow::Error| {
        Failure::new(
            CODE_EXECUTION,
            format!("check the node's incident relationships: {e:#}"),
        )
    };
    if !engine.outgoing_adj(id).map_err(map_err)?.is_empty() {
        return Ok(true);
    }
    if !engine.incoming_adj(id).map_err(map_err)?.is_empty() {
        return Ok(true);
    }
    Ok(false)
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

/// Execute one durable write: build the WAL op sequence from the parsed statement +
/// parameters, resolve the anchor's business key to a current-core dense id, and hand
/// the ops to the writer (WAL append + fsync commit + memtable apply + publish) as one
/// group commit. A statement lowers to several ops only when it mixes a replace-all with
/// further SET items; they commit atomically. Returns an empty result — read-back is a
/// separate `MATCH … RETURN` over the overlaid view.
fn execute_write(
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
                .map_or(true, |last| !last.cmp_key(&key_values[ri]).is_eq())
            {
                distinct.push(&key_values[ri]);
            }
            row_to_distinct[ri] = distinct.len() - 1;
        }
        let resolved = resolve_business_keys_batch(gen, &stmt.label, &stmt.key, &distinct);
        row_to_distinct.iter().map(|&d| resolved[d]).collect()
    };

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
                if node_has_relationships(writer, gen, id)? {
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
    let adj = engine.outgoing_adj(src).map_err(|e| {
        Failure::new(
            CODE_EXECUTION,
            format!("check for an existing relationship: {e:#}"),
        )
    })?;
    Ok(adj
        .iter()
        .find(|a| a.reltype == reltype && a.neighbour.0 == dst)
        .map(|a| a.edge.0))
}

/// Execute one durable relationship write (Phase 3c): resolve both endpoints, build
/// the WAL edge op, and hand it to the writer. A `MERGE` of an edge that already
/// exists in the core is an idempotent no-op; the relationship type must already
/// exist (the traversal overlay maps it to a core reltype id).
fn execute_edge_write(
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
        // small without an O(core) consolidation. Off by default (0); suppressed under
        // off-heap L0 (that flush still bails — an unsupported-path warn every write is not
        // worth spawning).
        if ctx.segment_flush_bytes > 0
            && !ctx.off_heap_l0
            && writer.total_bytes() >= ctx.segment_flush_bytes
        {
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

/// Whether an error is a lost `begin_consolidation` single-flight race — a flush or a
/// compaction that found another flush/consolidation already holding the exclusive claim
/// (`bail!("… is already in progress")`). Benign: the other op is doing the work, so the
/// segment-tier auto-triggers log this at debug rather than warn.
fn is_already_in_progress(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("already in progress")
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
            Err(e) if e.message.contains("already in progress") => {
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
    .map_err(|e| Failure::new(CODE_EXECUTION, format!("consolidation failed: {e:#}")))?;

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
/// query's whole life and the epoch that keys its cached result. `empty()` is the
/// read-only path (no delta, epoch 0), behaviourally identical to reading the core.
struct ReadOverlay {
    delta: DeltaSnapshot,
    epoch: u64,
}

impl ReadOverlay {
    fn empty() -> Self {
        Self {
            delta: DeltaSnapshot::empty(),
            epoch: 0,
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
    } = overlay;
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
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
    let fanout_pool = ctx.fanout_pool.clone();
    let beam_width = ctx.beam_width;
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
mod tests {
    use super::*;
    use crate::acl::hash_password;
    use crate::testgen;
    use tokio::net::TcpStream;

    /// Micro-benchmark isolating the write-resolve cost: time `resolve_business_key`
    /// over the 30%-delete segment (`wikidata_id` in `0..=p30`, ascending), cached vs
    /// uncached, against a real large core — no WAL/memtable/flush machinery. Answers
    /// "is the ISAM resolve the bulk-delete bottleneck, and does the range cache fix it?".
    /// Env-gated + `#[ignore]`; point it at a data dir:
    /// `SLATER_SMOKE_DATADIR=/home/rickk/perf-gens/wiki1m SLATER_SMOKE_GRAPH=wiki1m \
    ///   cargo test -p slater --lib bench_resolve_business_key -- --ignored --nocapture`
    #[test]
    #[ignore = "needs a prebuilt generation; see SLATER_SMOKE_DATADIR"]
    fn bench_resolve_business_key_over_the_segment() {
        let data_dir = std::env::var("SLATER_SMOKE_DATADIR")
            .expect("set SLATER_SMOKE_DATADIR to a slater data directory");
        let graph = std::env::var("SLATER_SMOKE_GRAPH").unwrap_or_else(|_| "wiki1m".to_string());
        let p30: i64 = std::env::var("SLATER_SMOKE_P30")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(332894);
        // Sample size — a small ascending run reproduces the "re-probe the same block"
        // pattern without a 10-minute loop. Default 5000.
        let n: i64 = std::env::var("SLATER_SMOKE_BENCH_N")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5000);
        let store: Arc<dyn ObjectStore> = Arc::new(FsObjectStore::new(&data_dir));

        let run = |label: &str, budget: Option<usize>| {
            // verify_integrity = false: the copy-completeness re-hash of a 1M-node core
            // would dwarf the loop we are timing (and the server pays it once at boot).
            let t_open = std::time::Instant::now();
            let gen = Generation::open_with_store_opts_cached(
                store.as_ref(),
                &graph,
                None,
                false,
                budget,
            )
            .expect("open generation");
            let open_elapsed = t_open.elapsed();
            // Index geometry — few big blocks ⇒ decode-per-probe dominates.
            if let Some(r) = gen.range_index("node_Entity_wikidata_id") {
                println!("  index blocks = {}", r.num_blocks());
            }
            let lo = p30 - n + 1;
            let t0 = std::time::Instant::now();
            let mut hits = 0u64;
            for k in lo..=p30 {
                if let KeyResolution::Unique(_) =
                    resolve_business_key(&gen, "Entity", "wikidata_id", &Value::Int(k))
                {
                    hits += 1;
                }
            }
            let loop_elapsed = t0.elapsed();
            println!(
                "{label}: open {open_elapsed:?}; per-row resolved {n} keys ({hits} hits) in \
                 {loop_elapsed:?} ({:.1} µs/resolve)",
                loop_elapsed.as_micros() as f64 / n as f64
            );

            // The batch merge-join resolve (slice 6.3): sweep the same ascending run once
            // instead of one point probe per key. Same verdicts, one decompress per touched
            // block for the whole batch — the bulk-write floor fix.
            let values: Vec<Value> = (lo..=p30).map(Value::Int).collect();
            let refs: Vec<&Value> = values.iter().collect();
            let t1 = std::time::Instant::now();
            let batch = resolve_business_keys_batch(&gen, "Entity", "wikidata_id", &refs);
            let batch_elapsed = t1.elapsed();
            let batch_hits = batch
                .iter()
                .filter(|r| matches!(r, KeyResolution::Unique(_)))
                .count();
            assert_eq!(batch_hits as u64, hits, "batch verdicts match per-row");
            println!(
                "{label}: batch-resolved {n} keys ({batch_hits} hits) in {batch_elapsed:?} \
                 ({:.1} µs/resolve, {:.1}× per-row)",
                batch_elapsed.as_micros() as f64 / n as f64,
                loop_elapsed.as_secs_f64() / batch_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
            );
        };

        run("uncached", None);
        run("cached-16MiB", Some(16 * 1024 * 1024));
    }

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

    /// A `DeltaConfig` with the writable layer on and a throwaway WAL directory.
    fn delta_cfg(wal_dir: &Path) -> DeltaConfig {
        DeltaConfig {
            enabled: true,
            wal_dir: wal_dir.to_string_lossy().into_owned(),
            memtable_bytes: 64 << 20,
            l0_compaction_trigger: 4,
            segment_flush_bytes: 0,
            max_upper_segments: 8,
            delta_core_percent: 0,
            delta_hard_bytes: 0,
            consolidate_window: String::new(),
            builder_bin: "slater-build".to_string(),
            off_heap_l0: false,
            segment_gc_grace_secs: 0,
        }
    }

    /// End-to-end Phase 1c: a business-key `SET` resolves the anchor to a core
    /// dense id, is durably logged + folded into the memtable, and a subsequent
    /// read sees the overwrite through the overlay — read-your-writes — with the
    /// value surviving a writer reopen (WAL replay).
    #[test]
    fn write_then_read_your_writes_and_survives_reopen() {
        let (root, _g, _) = testgen::write_basic("ryow");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").expect("writable layer is on");
        let epoch0 = writer.epoch();

        // Overwrite Alice's age and add a new property.
        let stmt = match parser::parse_statement(
            "MATCH (n:Person {name: 'Alice'}) SET n.age = 99, n.rating = 'AAA'",
        )
        .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
        let out = execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        assert_eq!(
            out,
            (Vec::new(), Vec::new()),
            "a no-RETURN write acks empty"
        );
        assert!(writer.epoch() > epoch0, "the write bumps the delta epoch");

        // The write resolved Alice to dense id 0 and folded the patch.
        let snap = writer.snapshot();
        let d = snap.node_patch(0).expect("resolved by dense id");
        assert_eq!(d.patches.get("age"), Some(&Value::Int(99)));
        drop(snap);

        // Read-your-writes through the merged view.
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age, n.rating").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        assert_eq!(res.rows.len(), 1);
        assert!(
            matches!(res.rows[0][0], Val::Int(99)),
            "overwritten age read back"
        );
        assert!(
            matches!(&res.rows[0][1], Val::Str(s) if s == "AAA"),
            "new property read back"
        );

        // Durability: a fresh writer over the same WAL replays the committed write.
        drop(writer);
        let reopened = DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            |op| resolve_op(&gen, op),
        )
        .unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .node_patch(0)
                .unwrap()
                .patches
                .get("age"),
            Some(&Value::Int(99)),
            "the write is durable across a reopen (WAL replay)"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// A write whose business key matches no existing node (or is not range-indexed)
    /// is a clean execution error, and a `RETURN` after `SET` is refused for now.
    #[test]
    fn write_errors_are_clean() {
        let (root, _g, _) = testgen::write_basic("write_err");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();

        // No such Person: a MATCH … SET on an absent key is an error (MATCH does not
        // create — the message points at MERGE, which does).
        let absent = match parser::parse_statement("MATCH (n:Person {name:'Nobody'}) SET n.age = 1")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
        let e = execute_write(&writer, gen.as_ref(), &absent, &HashMap::new()).unwrap_err();
        assert!(
            e.message.contains("node to update") && e.message.contains("MERGE"),
            "got: {}",
            e.message
        );

        // RETURN after SET is not yet supported.
        let with_ret =
            match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 1 RETURN n")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
        let e = execute_write(&writer, gen.as_ref(), &with_ret, &HashMap::new()).unwrap_err();
        assert!(
            e.message.contains("RETURN after a write"),
            "got: {}",
            e.message
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// End-to-end Phase 2b: a business-key `DELETE` tombstones the anchor; a
    /// subsequent read no longer binds it (read-your-deletes), a whole-label count
    /// drops it (the count fast path falls back to a real scan under a live delta),
    /// and the tombstone survives a writer reopen (WAL replay).
    #[test]
    fn delete_then_read_suppresses_node_and_survives_reopen() {
        let (root, _g, _) = testgen::write_basic("delete_ryow");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        // Helpers reading through the live overlay.
        let alice_rows = |w: &Arc<DeltaWriter>| -> usize {
            let view = MergedView::new(gen.as_ref(), DeltaSnapshot::from_memtable(w.snapshot()));
            let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.name").unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            res.rows.len()
        };
        let person_count = |w: &Arc<DeltaWriter>| -> i64 {
            let view = MergedView::new(gen.as_ref(), DeltaSnapshot::from_memtable(w.snapshot()));
            let ast = parser::parse("MATCH (n:Person) RETURN count(*)").unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            match res.rows[0][0] {
                Val::Int(n) => n,
                ref v => panic!("count not int: {v:?}"),
            }
        };

        // Baseline: Alice present, 3 Person nodes (Alice, Bob, Carol).
        assert_eq!(alice_rows(&writer), 1);
        assert_eq!(person_count(&writer), 3);

        // Delete Alice.
        // DETACH: Alice still has outgoing :KNOWS edges, so a plain DELETE would be
        // rejected (DELETE conformance); DETACH removes the node and detaches its edges.
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Alice'}) DETACH DELETE n")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

        // Read-your-deletes: the anchor scan no longer yields Alice, the count drops,
        // and her tombstone is stored under dense id 0.
        assert_eq!(alice_rows(&writer), 0, "Alice suppressed after delete");
        assert_eq!(person_count(&writer), 2, "tombstoned node not counted");
        assert!(writer.snapshot().node_patch(0).unwrap().tombstoned);

        // Durability: a fresh writer over the same WAL replays the tombstone.
        drop(writer);
        let reopened = DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            |op| resolve_op(&gen, op),
        )
        .unwrap();
        assert!(
            reopened.snapshot().node_patch(0).unwrap().tombstoned,
            "the delete is durable across a reopen (WAL replay)"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// End-to-end Phase 2c: a `MERGE` on an absent business key creates a delta-born
    /// node with a synthetic dense id. It reads back through a label scan, grows the
    /// whole-label count, and survives a writer reopen (WAL replay re-allocates the
    /// same synthetic id). A `MERGE` on an *existing* key patches it in place (no
    /// duplicate). NB: addressing a born node by an *indexed* key seek
    /// (`MATCH (n:Person {name:'Dave'})`) needs the Phase 2d index overlay — until
    /// then a born node is found by a label scan, not a range-index probe.
    #[test]
    fn merge_creates_delta_born_node_and_survives_reopen() {
        let (root, _g, _) = testgen::write_basic("merge_create");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        // Read all Person (name, age) rows through the live overlay (a label scan, so
        // it enumerates core nodes then delta-born ones).
        let people = |w: &Arc<DeltaWriter>| -> Vec<(String, Option<i64>)> {
            let view = MergedView::new(gen.as_ref(), DeltaSnapshot::from_memtable(w.snapshot()));
            let ast = parser::parse("MATCH (n:Person) RETURN n.name, n.age").unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            res.rows
                .iter()
                .map(|r| {
                    let name = match &r[0] {
                        Val::Str(s) => s.clone(),
                        v => panic!("name not str: {v:?}"),
                    };
                    let age = match &r[1] {
                        Val::Int(n) => Some(*n),
                        Val::Null => None,
                        v => panic!("age not int/null: {v:?}"),
                    };
                    (name, age)
                })
                .collect()
        };

        let base = people(&writer);
        assert!(
            !base.iter().any(|(n, _)| n == "Dave"),
            "Dave absent at start"
        );
        let base_n = base.len();

        // Create Dave via MERGE on an absent business key.
        let stmt = match parser::parse_statement("MERGE (n:Person {name:'Dave'}) SET n.age = 50")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
        assert!(stmt.upsert, "MERGE lowers to an upsert anchor");
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

        // Read-your-writes: Dave appears in the label scan with both his business-key
        // (name) and his SET property (age), and the count grew by exactly one.
        let after = people(&writer);
        assert_eq!(after.len(), base_n + 1, "count grew by one");
        assert!(
            after.contains(&("Dave".to_string(), Some(50))),
            "born Dave reads back with name+age: {after:?}"
        );

        // MERGE on an existing key patches in place (no second Bob).
        let patch = match parser::parse_statement("MERGE (n:Person {name:'Bob'}) SET n.age = 123")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
        execute_write(&writer, gen.as_ref(), &patch, &HashMap::new()).unwrap();
        let patched = people(&writer);
        assert_eq!(
            patched.len(),
            base_n + 1,
            "MERGE on an existing key does not duplicate"
        );
        assert_eq!(
            patched.iter().filter(|(n, _)| n == "Bob").count(),
            1,
            "exactly one Bob"
        );
        assert!(
            patched.contains(&("Bob".to_string(), Some(123))),
            "Bob patched in place: {patched:?}"
        );

        // Durability: a fresh writer over the same WAL replays create + patch, and the
        // born node keeps its synthetic id (allocation follows replay order).
        drop(writer);
        let reopened = DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            |op| resolve_op(&gen, op),
        )
        .unwrap();
        let reopened = Arc::new(reopened);
        let replayed = people(&reopened);
        assert!(
            replayed.contains(&("Dave".to_string(), Some(50))),
            "born Dave is durable across a reopen: {replayed:?}"
        );
        assert!(
            replayed.contains(&("Bob".to_string(), Some(123))),
            "patch is durable across a reopen: {replayed:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Deferred-from-2c: a `MERGE`-created (delta-born) node can be `DELETE`d by its
    /// business key even though it has no core row. The DELETE anchor's core probe
    /// returns `Absent`; the write path then resolves the born synthetic id from the
    /// delta and tombstones it. The node vanishes from reads and the whole-label count,
    /// deleting a genuinely-absent key is a clear error (not a silent no-op), and the
    /// delete is durable across a writer reopen (WAL replay).
    #[test]
    fn delete_removes_a_delta_born_node_by_key() {
        let (root, _g, _) = testgen::write_basic("delete_born");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        // Read the Person names through the full live overlay (label scan enumerating
        // core then delta-born nodes).
        let names = |w: &Arc<DeltaWriter>| -> Vec<String> {
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            res.rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("name not str: {v:?}"),
                })
                .collect()
        };
        let write = |w: &Arc<DeltaWriter>, q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(s) => s,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(w, gen.as_ref(), &stmt, &HashMap::new())
        };

        let base_n = names(&writer).len();
        assert!(
            !names(&writer).contains(&"Dave".to_string()),
            "Dave absent at start"
        );

        // Create Dave (delta-born), then DELETE him by his business key.
        write(&writer, "MERGE (n:Person {name:'Dave'}) SET n.age = 50").unwrap();
        assert!(
            names(&writer).contains(&"Dave".to_string()),
            "born Dave present after create"
        );
        assert_eq!(names(&writer).len(), base_n + 1, "count grew by one");

        write(&writer, "MATCH (n:Person {name:'Dave'}) DELETE n").unwrap();
        let after = names(&writer);
        assert!(
            !after.contains(&"Dave".to_string()),
            "born Dave gone after delete: {after:?}"
        );
        assert_eq!(after.len(), base_n, "count back to the baseline");

        // Deleting a business key absent from both core and delta is a clear error.
        let err = write(&writer, "MATCH (n:Person {name:'Nobody'}) DELETE n").unwrap_err();
        assert!(
            err.message
                .contains("no Person(name = …) node to delete: the business key matches no"),
            "clear no-such-node error: {}",
            err.message
        );

        // Durability: a fresh writer over the same WAL replays create + delete, so Dave
        // stays gone (the DELETE's born synthetic id re-resolves on replay).
        drop(writer);
        let reopened = Arc::new(
            DeltaWriter::open(
                wal.join("people"),
                "people",
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
                |op| resolve_op(&gen, op),
            )
            .unwrap(),
        );
        let replayed = names(&reopened);
        assert!(
            !replayed.contains(&"Dave".to_string()),
            "delete is durable across a reopen: {replayed:?}"
        );
        assert_eq!(replayed.len(), base_n, "count durable across a reopen");
        std::fs::remove_dir_all(&root).ok();
    }

    /// Write-UNWIND (group-commit surface): `UNWIND $rows AS r MERGE (n:Person {name:
    /// r.name}) SET n.age = r.age` creates one node per row under a **single** group
    /// commit (one epoch bump), each row's key + SET values evaluated against that row;
    /// a batched `MATCH … DELETE` likewise removes them. Durable across a reopen.
    #[test]
    fn write_unwind_batches_node_writes_under_one_commit() {
        let (root, _g) = testgen::write_indexed_people("unwind_batch");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let names = |w: &Arc<DeltaWriter>| -> Vec<String> {
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let res = Engine::new(&view, &cache)
                .run(&parser::parse("MATCH (n:Person) RETURN n.name").unwrap())
                .unwrap();
            let mut out: Vec<String> = res
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("name not str: {v:?}"),
                })
                .collect();
            out.sort();
            out
        };
        let age = |w: &Arc<DeltaWriter>, nm: &str| -> Vec<i64> {
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let q = format!("MATCH (n:Person {{name:'{nm}'}}) RETURN n.age");
            let res = Engine::new(&view, &cache)
                .run(&parser::parse(&q).unwrap())
                .unwrap();
            res.rows
                .iter()
                .filter_map(|r| match &r[0] {
                    Val::Int(n) => Some(*n),
                    _ => None,
                })
                .collect()
        };
        let run = |w: &Arc<DeltaWriter>, q: &str, params: &HashMap<String, Val>| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(s) => s,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(w, gen.as_ref(), &stmt, params).unwrap();
        };

        let base_n = names(&writer).len();
        // A parameter list of row maps — the bulk-import shape.
        let rows = Val::List(vec![
            Val::Map(vec![
                ("name".into(), Val::Str("Xavier".into())),
                ("age".into(), Val::Int(10)),
            ]),
            Val::Map(vec![
                ("name".into(), Val::Str("Yolanda".into())),
                ("age".into(), Val::Int(20)),
            ]),
        ]);
        let mut params = HashMap::new();
        params.insert("rows".to_string(), rows);

        // Batched create: two born nodes, ONE group-committed epoch.
        let e0 = writer.epoch();
        run(
            &writer,
            "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
            &params,
        );
        assert_eq!(
            writer.epoch(),
            e0 + 1,
            "the whole batch is one epoch (group commit)"
        );
        let after = names(&writer);
        assert_eq!(
            after.len(),
            base_n + 2,
            "two born nodes created by the batch"
        );
        assert!(after.contains(&"Xavier".to_string()) && after.contains(&"Yolanda".to_string()));
        assert_eq!(age(&writer, "Xavier"), vec![10], "per-row SET applied");
        assert_eq!(age(&writer, "Yolanda"), vec![20]);

        // Durable across a reopen (WAL replay reconstructs the batch).
        drop(writer);
        let reopened = Arc::new(
            DeltaWriter::open(
                wal.join("people"),
                "people",
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
                |op| resolve_op(&gen, op),
            )
            .unwrap(),
        );
        assert_eq!(age(&reopened, "Xavier"), vec![10], "batched writes durable");

        // Batched DELETE of the two born nodes via UNWIND (one epoch).
        let e1 = reopened.epoch();
        run(
            &reopened,
            "UNWIND $rows AS r MATCH (n:Person {name: r.name}) DELETE n",
            &params,
        );
        assert_eq!(reopened.epoch(), e1 + 1, "the batched delete is one epoch");
        let after_del = names(&reopened);
        assert!(
            !after_del.contains(&"Xavier".to_string())
                && !after_del.contains(&"Yolanda".to_string()),
            "batched delete removed both born nodes: {after_del:?}"
        );
        assert_eq!(after_del.len(), base_n, "count back to the baseline");
        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 2d: a range-index seek overlays the delta — an equality seek finds a
    /// delta-born node and drops a tombstoned core node, and a range seek unions the
    /// born node into the core hits. The fixture carries a `(Person, name)` index, so
    /// `MATCH (n:Person {name: …})` plans a `RangeEq` and `WHERE n.name >= …` a
    /// `RangeRange` (see `plan::choose_from_preds`) rather than a label scan — this is
    /// the path 2c's label-scan overlay did *not* cover.
    #[test]
    fn range_index_seek_overlays_born_and_tombstoned() {
        let (root, _g) = testgen::write_indexed_people("range_overlay_2d");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        // Run a query over the live overlay, returning the `name` column as a set.
        let names = |q: &str| -> Vec<String> {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let ast = parser::parse(q).unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            let mut out: Vec<String> = res
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("name not str: {v:?}"),
                })
                .collect();
            out.sort();
            out
        };

        // Baseline: an equality seek for the not-yet-created Dave finds nothing.
        assert!(
            names("MATCH (n:Person {name:'Dave'}) RETURN n.name").is_empty(),
            "Dave absent before MERGE"
        );

        // Create Dave (a delta-born node) and delete Bob (a core tombstone).
        let write = |q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };
        write("MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        // DETACH: Bob has an incident :KNOWS edge, so a plain DELETE would be rejected.
        write("MATCH (n:Person {name:'Bob'}) DETACH DELETE n");

        // RangeEq finds the born node — the headline 2d gap (a label scan already
        // found it in 2c; an *indexed key seek* did not until now).
        assert_eq!(
            names("MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"),
            vec!["Dave".to_string()],
            "equality seek finds the delta-born node"
        );
        // RangeEq drops the tombstoned core node.
        assert!(
            names("MATCH (n:Person {name:'Bob'}) RETURN n.name").is_empty(),
            "equality seek drops the tombstoned core node"
        );
        // RangeRange (n.name >= 'C') unions the born Dave with core Carol; Alice/Bob
        // are below the bound (and Bob is deleted regardless).
        assert_eq!(
            names("MATCH (n:Person) WHERE n.name >= 'C' RETURN n.name"),
            vec!["Carol".to_string(), "Dave".to_string()],
            "range seek unions the delta-born node into the core hits"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Follow-up from 2d ("moved indexed value"): a *core* node whose property patch
    /// changes an INDEXED value is relocated in the range index. `write_indexed_people`
    /// carries a (Person, name) RANGE index; patching Alice's `name` to 'Alicia' must
    /// move her — an equality seek finds her at the NEW value and misses her at the OLD
    /// one, and a range seek relocates her likewise. (The value read back was already
    /// correct via the property overlay; this closes the index-*membership* gap.)
    /// Durable across a writer reopen.
    #[test]
    fn moved_indexed_value_relocates_a_patched_core_node() {
        let (root, _g) = testgen::write_indexed_people("moved_index_2d");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let names = |w: &Arc<DeltaWriter>, q: &str| -> Vec<String> {
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let res = Engine::new(&view, &cache)
                .run(&parser::parse(q).unwrap())
                .unwrap();
            let mut out: Vec<String> = res
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("name not str: {v:?}"),
                })
                .collect();
            out.sort();
            out
        };

        // Baseline: Alice at her core value; nothing at 'Alicia'; a `>= 'Alicia'` range
        // excludes her (Alice < Alicia < Bob, Carol).
        assert_eq!(
            names(&writer, "MATCH (n:Person {name:'Alice'}) RETURN n.name"),
            vec!["Alice"]
        );
        assert!(names(&writer, "MATCH (n:Person {name:'Alicia'}) RETURN n.name").is_empty());
        assert_eq!(
            names(
                &writer,
                "MATCH (n:Person) WHERE n.name >= 'Alicia' RETURN n.name"
            ),
            vec!["Bob", "Carol"]
        );

        // Patch the indexed value: Alice → 'Alicia'.
        let stmt =
            match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.name = 'Alicia'")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a write"),
            };
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

        // Equality seek: found at the NEW value (moved in), missed at the OLD one (moved
        // out). The "moved in" is the load-bearing case — the relocated node is absent
        // from the core ISAM at 'Alicia', so without the overlay it is never a candidate.
        assert_eq!(
            names(&writer, "MATCH (n:Person {name:'Alicia'}) RETURN n.name"),
            vec!["Alicia"],
            "equality seek at the new indexed value finds the relocated node"
        );
        assert!(
            names(&writer, "MATCH (n:Person {name:'Alice'}) RETURN n.name").is_empty(),
            "equality seek at the old indexed value no longer finds it"
        );
        // Range seek relocates her into `[>= 'Alicia']`.
        assert_eq!(
            names(
                &writer,
                "MATCH (n:Person) WHERE n.name >= 'Alicia' RETURN n.name"
            ),
            vec!["Alicia", "Bob", "Carol"],
            "range seek unions the relocated core node into the hits"
        );

        // Durable across a reopen (WAL replay re-applies the patch onto the same dense id).
        drop(writer);
        let reopened = Arc::new(
            DeltaWriter::open(
                wal.join("people"),
                "people",
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
                |op| resolve_op(&gen, op),
            )
            .unwrap(),
        );
        assert_eq!(
            names(&reopened, "MATCH (n:Person {name:'Alicia'}) RETURN n.name"),
            vec!["Alicia"],
            "relocation is durable across a reopen"
        );
        assert!(names(&reopened, "MATCH (n:Person {name:'Alice'}) RETURN n.name").is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 3b: the traversal read overlay. A `MERGE`-created relationship is
    /// walkable (both directions), a deleted core edge no longer traverses, and an
    /// edge to a tombstoned node is suppressed (closing the 2b gap). Edges are written
    /// directly through the `DeltaWriter` (the write *grammar* is 3c) on the
    /// `write_indexed_people` fixture: Alice(0)-[:KNOWS]->Bob(1), plus Carol(2), with a
    /// `(Person, name)` index that resolves the anchors.
    #[test]
    fn edge_overlay_folds_born_and_deleted_edges() {
        let (root, _g) = testgen::write_indexed_people("edge_overlay_3b");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        // Run `q` over the live overlay, returning the single string column.
        let names = |q: &str| -> Vec<String> {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let ast = parser::parse(q).unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            let mut out: Vec<String> = res
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("expected str, got {v:?}"),
                })
                .collect();
            out.sort();
            out
        };
        let edge = |create: bool, src: u64, dst: u64| {
            let (sname, dname) = (
                ["Alice", "Bob", "Carol"][src as usize],
                ["Alice", "Bob", "Carol"][dst as usize],
            );
            let op = if create {
                WalOp::UpsertEdge {
                    src_label: "Person".into(),
                    src_key: "name".into(),
                    src_value: Value::Str(sname.into()),
                    reltype: "KNOWS".into(),
                    dst_label: "Person".into(),
                    dst_key: "name".into(),
                    dst_value: Value::Str(dname.into()),
                    patches: vec![],
                }
            } else {
                WalOp::DeleteEdge {
                    src_label: "Person".into(),
                    src_key: "name".into(),
                    src_value: Value::Str(sname.into()),
                    reltype: "KNOWS".into(),
                    dst_label: "Person".into(),
                    dst_key: "name".into(),
                    dst_value: Value::Str(dname.into()),
                }
            };
            writer
                .write(
                    op,
                    OpResolution::Edge {
                        src: Some(src),
                        dst: Some(dst),
                        edge_id: None,
                    },
                )
                .unwrap();
        };

        // Baseline: only the core edge Alice-KNOWS->Bob.
        assert_eq!(
            names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Bob".to_string()]
        );
        assert!(names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name").is_empty());

        // Create a born edge Bob-KNOWS->Carol: now traversable outgoing from Bob and
        // incoming to Carol.
        edge(true, 1, 2);
        assert_eq!(
            names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Carol".to_string()],
            "born edge is walkable outgoing"
        );
        assert_eq!(
            names("MATCH (a)-[:KNOWS]->(b:Person {name:'Carol'}) RETURN a.name"),
            vec!["Bob".to_string()],
            "born edge is walkable incoming"
        );

        // Delete the core edge Alice-KNOWS->Bob: it stops traversing (both directions).
        edge(false, 0, 1);
        assert!(
            names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").is_empty(),
            "deleted core edge no longer walks outgoing"
        );
        assert!(
            names("MATCH (a)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name").is_empty(),
            "deleted core edge no longer walks incoming"
        );
        // The born edge is unaffected by the unrelated delete.
        assert_eq!(
            names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Carol".to_string()]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 3b (the closed 2b gap): a core edge to a node deleted via the delta is no
    /// longer reachable by traversal — the node tombstone suppresses its incident core
    /// edges on read.
    #[test]
    fn edge_overlay_suppresses_edge_to_tombstoned_node() {
        let (root, _g) = testgen::write_indexed_people("edge_overlay_tomb_3b");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let hop = || -> Vec<String> {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let ast = parser::parse("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name")
                .unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            res.rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("expected str, got {v:?}"),
                })
                .collect()
        };

        assert_eq!(hop(), vec!["Bob".to_string()], "core edge reaches Bob");

        // Delete Bob (the edge's destination) through the write path. DETACH because Bob
        // still has the incident :KNOWS edge — a plain DELETE would be rejected.
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) DETACH DELETE n")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

        assert!(
            hop().is_empty(),
            "the core edge to the now-tombstoned Bob is suppressed"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// DELETE conformance (Stage 2): a plain `DELETE` of a node that still has
    /// relationships is rejected — in either edge direction — and leaves the node in
    /// place; `DETACH DELETE` removes the node and its edges.
    #[test]
    fn plain_delete_rejects_node_with_relationships_detach_allows() {
        let (root, _g) = testgen::write_indexed_people("delete_conformance_s2");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run = |q: &str| -> std::result::Result<(), Failure> {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
                }
                other => panic!("expected a node write for {q:?}, got {other:?}"),
            }
        };
        let present = |name: &str| -> bool {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.name");
            let ast = parser::parse(&q).unwrap();
            let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows.len();
            rows > 0
        };

        // Alice has an outgoing :KNOWS edge to Bob → a plain DELETE is rejected, and
        // Alice is untouched.
        let e = run("MATCH (n:Person {name:'Alice'}) DELETE n").unwrap_err();
        assert!(
            e.message.contains("still has relationships"),
            "got: {}",
            e.message
        );
        assert!(present("Alice"), "the rejected DELETE left Alice in place");

        // Bob has an *incoming* :KNOWS edge from Alice → a plain DELETE is rejected too
        // (the check sees both directions).
        let e = run("MATCH (n:Person {name:'Bob'}) DELETE n").unwrap_err();
        assert!(
            e.message.contains("still has relationships"),
            "got: {}",
            e.message
        );
        assert!(present("Bob"), "the rejected DELETE left Bob in place");

        // DETACH DELETE removes Alice and her edges; a subsequent plain DELETE of Bob
        // now succeeds (his only relationship was the edge from Alice, now gone).
        run("MATCH (n:Person {name:'Alice'}) DETACH DELETE n").unwrap();
        assert!(!present("Alice"), "DETACH DELETE removed Alice");
        run("MATCH (n:Person {name:'Bob'}) DELETE n").unwrap();
        assert!(
            !present("Bob"),
            "Bob had no remaining edges, so plain DELETE worked"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// End-to-end Stage 3: `REMOVE n.p` drops a property, `SET n = {map}` replaces all
    /// of them (the anchor business key survives), and touching the anchor key is
    /// rejected — all read back through the live overlay.
    #[test]
    fn remove_and_replace_read_back_through_the_overlay() {
        let (root, _g) = testgen::write_indexed_people("remove_replace_s3");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run = |q: &str| -> std::result::Result<(), Failure> {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
                }
                other => panic!("expected a node write for {q:?}, got {other:?}"),
            }
        };
        // A single property, read through the live overlay, rendered to a comparable
        // string (`Val` has no `PartialEq`): `null` / `int:N` / `str:S`.
        let prop = |name: &str, p: &str| -> String {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.{p}");
            let ast = parser::parse(&q).unwrap();
            let mut rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            match rows.pop().map(|mut r| r.remove(0)).unwrap_or(Val::Null) {
                Val::Null => "null".to_string(),
                Val::Int(n) => format!("int:{n}"),
                Val::Str(s) => format!("str:{s}"),
                other => format!("other:{other:?}"),
            }
        };

        // Seed Alice with a new property, then REMOVE it: the property reads back Null
        // while an untouched core property (age) is unaffected.
        run("MATCH (n:Person {name:'Alice'}) SET n.city = 'NYC'").unwrap();
        assert_eq!(prop("Alice", "city"), "str:NYC");
        run("MATCH (n:Person {name:'Alice'}) REMOVE n.city").unwrap();
        assert_eq!(prop("Alice", "city"), "null", "REMOVE drops the property");
        assert_eq!(
            prop("Alice", "age"),
            "int:30",
            "an untouched core prop stands"
        );

        // Replace-all on Bob: a prior property (city) is wiped, `age` is replaced, and the
        // anchor business key (name) survives even though the map omits it.
        run("MATCH (n:Person {name:'Bob'}) SET n.city = 'LA'").unwrap();
        run("MATCH (n:Person {name:'Bob'}) SET n = {age: 99}").unwrap();
        assert_eq!(prop("Bob", "age"), "int:99", "replace-all set the new age");
        assert_eq!(
            prop("Bob", "city"),
            "null",
            "replace-all wiped the old city"
        );
        assert_eq!(
            prop("Bob", "name"),
            "str:Bob",
            "the anchor business key survives a replace-all"
        );

        // The anchor key cannot be REMOVEd — it is the node's identity.
        let e = run("MATCH (n:Person {name:'Carol'}) REMOVE n.name").unwrap_err();
        assert!(e.message.contains("business-key"), "got: {}", e.message);
        // …but it may be re-set (here via replace-all), which relocates the node in the
        // index — it is then found at its new key value.
        run("MATCH (n:Person {name:'Carol'}) SET n = {name: 'Xavier'}").unwrap();
        assert_eq!(
            prop("Xavier", "name"),
            "str:Xavier",
            "replace-all relocated the node to its new key value"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// End-to-end Stage 4: `SET n += {map}` merges, multiple SET items fold in source
    /// order (last-writer-wins), and a replace-all mixed with a following SET
    /// group-commits (the post-replace patch lands on top of the replaced base).
    #[test]
    fn multi_item_and_merge_map_set_fold_in_source_order() {
        let (root, _g) = testgen::write_indexed_people("multi_set_s4");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run = |q: &str| {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                other => panic!("expected a node write for {q:?}, got {other:?}"),
            };
        };
        let prop = |name: &str, p: &str| -> String {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.{p}");
            let ast = parser::parse(&q).unwrap();
            let mut rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            match rows.pop().map(|mut r| r.remove(0)).unwrap_or(Val::Null) {
                Val::Null => "null".to_string(),
                Val::Int(n) => format!("int:{n}"),
                Val::Str(s) => format!("str:{s}"),
                other => format!("other:{other:?}"),
            }
        };

        // `SET n += {map}` adds every entry.
        run("MATCH (n:Person {name:'Alice'}) SET n += {city: 'NYC', role: 'eng'}");
        assert_eq!(prop("Alice", "city"), "str:NYC");
        assert_eq!(prop("Alice", "role"), "str:eng");

        // Mixed items fold in source order, last-writer-wins across Prop and merge-map.
        run("MATCH (n:Person {name:'Bob'}) SET n.score = 1, n += {score: 2, tier: 'A'}, n.tier = 'B'");
        assert_eq!(
            prop("Bob", "score"),
            "int:2",
            "the later merge-map value wins over the earlier prop"
        );
        assert_eq!(
            prop("Bob", "tier"),
            "str:B",
            "the later prop wins over the merge-map"
        );

        // A replace-all mixed with a following SET group-commits: the replace wipes the
        // earlier property, then the post-replace patch lands on top.
        run("MATCH (n:Person {name:'Carol'}) SET n.old = 'x'");
        run("MATCH (n:Person {name:'Carol'}) SET n = {age: 50}, n.city = 'LA'");
        assert_eq!(prop("Carol", "age"), "int:50", "replace set the new age");
        assert_eq!(
            prop("Carol", "city"),
            "str:LA",
            "the post-replace SET applied on top"
        );
        assert_eq!(
            prop("Carol", "old"),
            "null",
            "the replace wiped the earlier property"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// End-to-end Stage 5: `SET n:Label` / `REMOVE n:Label` change what a node matches and
    /// scans as, the label counts stay **exact** under the overlay (no fall-back scan),
    /// the first-label grouping re-buckets, and the guards (brand-new label, born identity
    /// label) fire.
    #[test]
    fn label_mutation_matches_scans_counts_and_validates() {
        let (root, _g, _) = testgen::write_basic("label_mut_s5");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run = |q: &str| -> std::result::Result<(), Failure> {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
                }
                other => panic!("expected a node write for {q:?}, got {other:?}"),
            }
        };
        let view = || {
            MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            )
        };
        let names = |q: &str| -> Vec<String> {
            let v = view();
            let ast = parser::parse(q).unwrap();
            let mut out: Vec<String> = Engine::new(&v, &cache)
                .run(&ast)
                .unwrap()
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    other => panic!("expected str, got {other:?}"),
                })
                .collect();
            out.sort();
            out
        };
        let count = |q: &str| -> i64 {
            let v = view();
            let ast = parser::parse(q).unwrap();
            let n = match Engine::new(&v, &cache).run(&ast).unwrap().rows[0][0] {
                Val::Int(n) => n,
                ref other => panic!("count not int: {other:?}"),
            };
            n
        };

        let base_person = count("MATCH (n:Person) RETURN count(*)");
        let base_company = count("MATCH (n:Company) RETURN count(*)");

        // SET n:Company on a Person → it now matches and scans as :Company, and the exact
        // label count grows by one; it still matches :Person.
        run("MATCH (n:Person {name:'Alice'}) SET n:Company").unwrap();
        assert!(names("MATCH (n:Company) RETURN n.name").contains(&"Alice".to_string()));
        assert_eq!(
            count("MATCH (n:Company) RETURN count(*)"),
            base_company + 1,
            "exact label count reflects the added label under the overlay"
        );
        assert!(names("MATCH (n:Person) RETURN n.name").contains(&"Alice".to_string()));
        assert_eq!(
            count("MATCH (n:Person) RETURN count(*)"),
            base_person,
            "Person count is unchanged (Alice kept :Person)"
        );

        // REMOVE it → back to the baseline.
        run("MATCH (n:Person {name:'Alice'}) REMOVE n:Company").unwrap();
        assert!(!names("MATCH (n:Company) RETURN n.name").contains(&"Alice".to_string()));
        assert_eq!(count("MATCH (n:Company) RETURN count(*)"), base_company);

        // Removing the identity label of an existing **core** node is allowed; the exact
        // Person count drops, and the node re-buckets to the null first-label group.
        run("MATCH (n:Person {name:'Bob'}) REMOVE n:Person").unwrap();
        assert!(!names("MATCH (n:Person) RETURN n.name").contains(&"Bob".to_string()));
        assert_eq!(
            count("MATCH (n:Person) RETURN count(*)"),
            base_person - 1,
            "exact label count reflects the dropped label"
        );
        // First-label grouping re-buckets Bob from Person to null.
        let group = |first: &str| -> i64 {
            let v = view();
            let q = format!(
                "MATCH (n) WITH labels(n)[0] AS l, count(*) AS c WHERE l = '{first}' RETURN c"
            );
            let ast = parser::parse(&q).unwrap();
            let rows = Engine::new(&v, &cache).run(&ast).unwrap().rows;
            match rows.first().map(|r| &r[0]) {
                Some(Val::Int(n)) => *n,
                _ => 0,
            }
        };
        assert_eq!(
            group("Person"),
            base_person - 1,
            "the first-label Person group loses Bob"
        );

        // A brand-new label (absent from the core symbol table) is rejected by name.
        let e = run("MATCH (n:Person {name:'Carol'}) SET n:Ghost").unwrap_err();
        assert!(e.message.contains("not defined"), "got: {}", e.message);

        // A delta-born node's identity label cannot be removed.
        run("MERGE (n:Person {name:'Zoe'}) SET n.age = 1").unwrap();
        let e = run("MATCH (n:Person {name:'Zoe'}) REMOVE n:Person").unwrap_err();
        assert!(e.message.contains("identity label"), "got: {}", e.message);

        std::fs::remove_dir_all(&root).ok();
    }

    /// End-to-end Stage 7: `CREATE` makes a node from its inline props (business key = the
    /// range-indexed one); `MERGE … ON CREATE / ON MATCH SET` fire the right branch by
    /// whether the node was created or matched.
    #[test]
    fn create_and_merge_conditional_sets_end_to_end() {
        let (root, _g) = testgen::write_indexed_people("stage7");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run = |q: &str| -> std::result::Result<(), Failure> {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
                }
                parser::ast::Statement::Create(c) => {
                    execute_create(&writer, gen.as_ref(), &c, &HashMap::new()).map(|_| ())
                }
                other => panic!("expected a write/create for {q:?}, got {other:?}"),
            }
        };
        let prop = |name: &str, p: &str| -> String {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.{p}");
            let ast = parser::parse(&q).unwrap();
            let mut rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            match rows.pop().map(|mut r| r.remove(0)).unwrap_or(Val::Null) {
                Val::Null => "null".to_string(),
                Val::Int(n) => format!("int:{n}"),
                Val::Str(s) => format!("str:{s}"),
                other => format!("other:{other:?}"),
            }
        };

        // CREATE makes a node with its inline properties (name is the range-indexed key).
        run("CREATE (n:Person {name: 'Zoe', age: 20})").unwrap();
        assert_eq!(
            prop("Zoe", "age"),
            "int:20",
            "CREATE made the node with its props"
        );

        // MERGE on an absent key → ON CREATE fires.
        run("MERGE (n:Person {name: 'Yan'}) ON CREATE SET n.origin = 'created' ON MATCH SET n.origin = 'matched'").unwrap();
        assert_eq!(
            prop("Yan", "origin"),
            "str:created",
            "ON CREATE fired for a new node"
        );

        // MERGE on an existing core key (Alice) → ON MATCH fires.
        run("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.origin = 'created' ON MATCH SET n.origin = 'matched'").unwrap();
        assert_eq!(
            prop("Alice", "origin"),
            "str:matched",
            "ON MATCH fired for an existing node"
        );

        // Re-MERGE Yan → it now matches the delta-born node created above.
        run("MERGE (n:Person {name: 'Yan'}) ON CREATE SET n.origin = 'c2' ON MATCH SET n.origin = 'm2'").unwrap();
        assert_eq!(
            prop("Yan", "origin"),
            "str:m2",
            "the second MERGE matched the born node"
        );

        // CREATE with no range-indexed property among its props is rejected.
        let e = run("CREATE (n:Person {city: 'X'})").unwrap_err();
        assert!(
            e.message.contains("range-indexed business key"),
            "got: {}",
            e.message
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 3c: the relationship write grammar, end to end. `MERGE (a)-[:R]->(b)`
    /// creates a walkable edge (idempotent against an existing core edge, and
    /// auto-creating an absent endpoint); `MATCH (a)-[r:R]->(b) DELETE r` removes one;
    /// an unknown relationship type is rejected.
    #[test]
    fn edge_write_grammar_end_to_end() {
        let (root, _g) = testgen::write_indexed_people("edge_write_3c");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run_write = |q: &str| -> std::result::Result<(), Failure> {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
                }
                other => panic!("expected an edge write for {q:?}, got {other:?}"),
            }
        };
        let names = |q: &str| -> Vec<String> {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let ast = parser::parse(q).unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            let mut out: Vec<String> = res
                .rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("expected str, got {v:?}"),
                })
                .collect();
            out.sort();
            out
        };

        // Create Bob-KNOWS->Carol.
        run_write("MERGE (a:Person {name:'Bob'})-[:KNOWS]->(b:Person {name:'Carol'})").unwrap();
        assert_eq!(
            names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Carol".to_string()]
        );

        // Idempotent MERGE of the existing core edge Alice-KNOWS->Bob: no duplicate.
        run_write("MERGE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})").unwrap();
        assert_eq!(
            names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Bob".to_string()],
            "MERGE of an existing core edge does not duplicate it"
        );

        // MERGE with an absent destination auto-creates the born node + edge.
        run_write("MERGE (a:Person {name:'Bob'})-[:KNOWS]->(b:Person {name:'Zoe'})").unwrap();
        assert_eq!(
            names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Carol".to_string(), "Zoe".to_string()],
            "born endpoint Zoe is created and reachable"
        );
        assert!(
            names("MATCH (n:Person) RETURN n.name").contains(&"Zoe".to_string()),
            "born endpoint Zoe is a Person node"
        );

        // Delete the core edge Alice-KNOWS->Bob.
        run_write("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r")
            .unwrap();
        assert!(
            names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").is_empty(),
            "the deleted core edge no longer traverses"
        );

        // An unknown relationship type is rejected.
        let err = run_write("MERGE (a:Person {name:'Alice'})-[:NOPE]->(b:Person {name:'Carol'})")
            .unwrap_err();
        assert!(
            err.message.contains("must already exist"),
            "unknown reltype rejected: {}",
            err.message
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 3c durability: a created edge and a deleted core edge survive a WAL
    /// reopen — the edge WAL ops replay and re-resolve their endpoints deterministically
    /// (born endpoints re-allocate their synthetic ids in replay order).
    #[test]
    fn edge_writes_survive_a_reopen() {
        let (root, _g) = testgen::write_indexed_people("edge_durable_3c");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        {
            let writer = graphs.writer("people").unwrap();
            // Create Bob-KNOWS->Carol and delete the core Alice-KNOWS->Bob.
            let mk = |q: &str| match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected an edge write"),
            };
            mk("MERGE (a:Person {name:'Bob'})-[:KNOWS]->(b:Person {name:'Carol'})");
            mk("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r");
        }

        // Reopen the writer over the same WAL and re-run the reads over the fresh delta.
        let reopened = Arc::new(
            DeltaWriter::open(
                wal.join("people"),
                "people",
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
                |op| resolve_op(&gen, op),
            )
            .unwrap(),
        );
        let names = |q: &str| -> Vec<String> {
            let view = MergedView::new(
                gen.as_ref(),
                DeltaSnapshot::from_memtable(reopened.snapshot()),
            );
            let ast = parser::parse(q).unwrap();
            let res = Engine::new(&view, &cache).run(&ast).unwrap();
            res.rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("expected str, got {v:?}"),
                })
                .collect()
        };
        assert_eq!(
            names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
            vec!["Carol".to_string()],
            "created edge is durable"
        );
        assert!(
            names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").is_empty(),
            "deleted edge stays deleted across a reopen"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Edge properties (follow-up from 3c): `MERGE (a)-[r:R]->(b) SET r.p = …` gives a
    /// delta-born edge properties; a re-`MERGE` patches them in place; they read back via
    /// `RETURN r.p`, and survive a reopen. Patching a *core* edge's properties in place is
    /// now supported too — a `SET` on an existing core edge updates it, a bare re-`MERGE`
    /// stays an idempotent no-op, and the patch replays across a reopen. (`write_indexed_people`
    /// carries a core edge Alice-KNOWS->Bob with `since = 2020`.)
    #[test]
    fn edge_properties_end_to_end() {
        let (root, _g) = testgen::write_indexed_people("edge_props_3");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let run_write = |q: &str| -> std::result::Result<(), Failure> {
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
                }
                other => panic!("expected an edge write for {q:?}, got {other:?}"),
            }
        };
        // Read a single scalar column over the live overlay (Int, or -1 for Null).
        let scalar = |w: &Arc<DeltaWriter>, q: &str| -> Vec<i64> {
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let res = Engine::new(&view, &cache)
                .run(&parser::parse(q).unwrap())
                .unwrap();
            res.rows
                .iter()
                .map(|r| match &r[0] {
                    Val::Int(n) => *n,
                    Val::Null => -1,
                    v => panic!("expected int/null, got {v:?}"),
                })
                .collect()
        };

        // Create a born edge Bob-KNOWS->Carol with a property.
        run_write(
            "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) SET r.since = 1999",
        )
        .unwrap();
        assert_eq!(
            scalar(
                &writer,
                "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.since"
            ),
            vec![1999],
            "born edge property reads back"
        );

        // Re-MERGE patches the property in place and adds a second one (no duplicate edge).
        run_write(
            "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) SET r.since = 2000, r.weight = 5",
        )
        .unwrap();
        assert_eq!(
            scalar(
                &writer,
                "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.since"
            ),
            vec![2000],
            "re-MERGE patches the property"
        );
        assert_eq!(
            scalar(
                &writer,
                "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.weight"
            ),
            vec![5],
            "a second property is added"
        );

        // Patching a CORE edge's properties in place now updates it (was rejected before).
        assert_eq!(
            scalar(
                &writer,
                "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
            ),
            vec![2020],
            "the core edge's original property reads from the core"
        );
        run_write(
            "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 7",
        )
        .unwrap();
        assert_eq!(
            scalar(
                &writer,
                "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
            ),
            vec![7],
            "the core edge's property is patched in place"
        );
        // A bare re-MERGE of that same core edge is still an idempotent no-op — the patch stands.
        run_write("MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'})").unwrap();
        assert_eq!(
            scalar(
                &writer,
                "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
            ),
            vec![7],
            "a bare re-MERGE leaves the core-edge patch intact"
        );

        // Durable across a reopen: the born edge's patched properties AND the core-edge
        // patch replay (the latter re-resolves its core edge id via `resolve_op`).
        drop(writer);
        let reopened = Arc::new(
            DeltaWriter::open(
                wal.join("people"),
                "people",
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
                |op| resolve_op(&gen, op),
            )
            .unwrap(),
        );
        assert_eq!(
            scalar(
                &reopened,
                "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.since"
            ),
            vec![2000],
            "born edge properties are durable across a reopen"
        );
        assert_eq!(
            scalar(
                &reopened,
                "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
            ),
            vec![7],
            "the core-edge patch is durable across a reopen"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// The result-cache key includes the delta epoch, so a write invalidates an
    /// overlaid result rather than serving it stale.
    #[test]
    fn result_key_binds_delta_epoch() {
        let g = GenId(uuid::Uuid::from_u128(7));
        let k0 = ResultKey::with_delta_epoch(g, 0, "q");
        let k1 = ResultKey::with_delta_epoch(g, 1, "q");
        assert_ne!(k0, k1, "a bumped epoch keys differently");
        assert_eq!(k0, ResultKey::new(g, "q"), "epoch 0 == the read-only key");
    }

    /// Count the `*.wal` segment files under a WAL directory.
    fn wal_count(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("wal"))
            .count()
    }

    /// Read a binary consolidation dump into a `{ node name → node props }` map, for
    /// tests that assert the serialiser saw the merged state. Nodes are keyed by their
    /// `name` property (the fixtures' business key).
    fn dump_nodes(
        dump: &Path,
    ) -> std::collections::HashMap<String, Vec<(String, graph_format::ids::Value)>> {
        use graph_format::consolidate_dump::DumpReader;
        let r = DumpReader::open(dump).unwrap();
        let keys = r.meta().property_keys.clone();
        let mut out = std::collections::HashMap::new();
        r.for_each_node(|_, _lb, pb| {
            let props: Vec<(String, graph_format::ids::Value)> =
                graph_format::columns::decode_props(pb)
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| (keys[k as usize].clone(), v))
                    .collect();
            if let Some((_, graph_format::ids::Value::Str(name))) =
                props.iter().find(|(k, _)| k == "name")
            {
                out.insert(name.clone(), props);
            }
            Ok(())
        })
        .unwrap();
        out
    }

    /// The integer `age` of node `name` in a binary dump, if present.
    fn dump_age(dump: &Path, name: &str) -> Option<i64> {
        dump_nodes(dump).get(name).and_then(|p| {
            p.iter()
                .find(|(k, _)| k == "age")
                .and_then(|(_, v)| match v {
                    graph_format::ids::Value::Int(i) => Some(*i),
                    _ => None,
                })
        })
    }

    /// End-to-end Phase 1d-B: a durable delta is folded into a fresh generation by
    /// consolidation. The injected builder inspects the dump (proving the serialiser
    /// saw the *merged* state) and independently publishes the known-correct
    /// consolidated generation; afterwards the served core carries the write with no
    /// delta, the writer is re-bound to the new core, and the consumed WAL segments
    /// are gone — leaving only the fresh post-freeze segment.
    #[test]
    fn consolidate_folds_delta_into_fresh_generation() {
        let (root, _graph) = testgen::write_indexed_people("consolidate_e2e");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen0 = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let wal_dir = writer.wal_dir();

        // Overwrite Alice's age via the delta.
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
        execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
        assert!(
            !writer.snapshot().is_empty(),
            "delta live before consolidation"
        );

        // Builder stand-in: assert the dump reflects the merged age, then — modelling a
        // client that keeps writing *during* the rebuild (freeze has happened, retire has
        // not) — apply a post-freeze write (Bob's age → 77) before publishing an
        // independently-correct consolidated generation (Alice age 99) at a new uuid. The
        // post-freeze write is deliberately absent from the dump, so it must be carried
        // forward onto the new core by retire (Phase 4a).
        let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0099);
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let writer_mid = writer.clone();
        let gen_mid = gen0.clone();
        let build = |dump: &Path, g: &str, dd: &Path| -> Result<()> {
            assert_eq!(
                dump_age(dump, "Alice"),
                Some(99),
                "dump should carry the merged age"
            );
            assert_ne!(
                dump_age(dump, "Bob"),
                Some(77),
                "the post-freeze write (Bob age 77) must not be in the frozen dump"
            );
            assert_eq!(g, "people");
            let bob = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
            execute_write(&writer_mid, gen_mid.as_ref(), &bob, &HashMap::new()).unwrap();
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            Ok(())
        };
        let published = graphs
            .consolidate_graph("people", &cache, &vc, &root, build)
            .unwrap();
        assert_eq!(published.0, new_uuid, "swapped to the new generation");

        // The served core is now the new generation with Alice's write baked in; the
        // post-freeze Bob write survived as a delta re-resolved onto the new core.
        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.uuid().0, new_uuid);
        assert!(
            !writer.snapshot().is_empty(),
            "the post-freeze write is carried forward, not dropped"
        );
        let read_age = |name: &str| -> Val {
            let view = MergedView::new(
                gen1.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let ast =
                parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
            let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
            age
        };
        assert!(
            matches!(read_age("Alice"), Val::Int(99)),
            "consolidated age served from the core"
        );
        assert!(
            matches!(read_age("Bob"), Val::Int(77)),
            "post-freeze write served from the carried-forward delta over the new core"
        );

        // The writer is re-bound to the new core; the scratch dump is cleaned up; only
        // the post-freeze segment remains (freeze's fresh segment, now holding Bob).
        assert_eq!(
            writer.core_uuid(),
            gen1.uuid(),
            "writer re-bound to new core"
        );
        assert!(!root.join("people").join(".consolidate.dump").exists());
        assert_eq!(
            wal_count(&wal_dir),
            1,
            "only the post-freeze segment remains"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 7 slice 7.1: the consolidation dump serialiser folds the **core stack**, so a
    /// retarget over a stacked set collapses it to a *correct* singleton. After a flush moves a
    /// base-node patch (Alice→99), a base-node delete (Carol), a born node (Dave) and a born
    /// edge (Dave→Bob) into one segment, dumping the served stacked generation with an empty
    /// delta must reflect the **segment** state — not the stale base bytes the Phase-0.5
    /// byte-copy fast path would emit. Concretely: Alice carries the segment's patched age
    /// (proving the fast path yields to the decode-through-stack slow path for a
    /// segment-overridden base id), Carol is elided and the survivors renumbered gaplessly
    /// (proving the segment tombstone joins the combined tombstone set that drives `compact_id`),
    /// and Dave + his born edge appear with compacted endpoints.
    #[test]
    fn consolidation_dump_folds_the_segment_stack() {
        let (root, _g) = testgen::write_indexed_people("retarget_dump_71");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        // A base-node patch, a base-node delete, a born node, and a born edge from the born
        // node to a surviving base node — every stack override kind in one flush.
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
        write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(
            &graphs,
            "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Bob'})",
        );
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");
        let gen = graphs.get("people").unwrap();
        assert_eq!(gen.stack().segments().len(), 1, "one upper segment");
        assert!(
            graphs.writer("people").unwrap().snapshot().is_empty(),
            "delta retired empty — the dump reads the stack alone"
        );

        // Dump the served *stacked* generation with an empty delta.
        let dir = root.join(".retarget71.dump");
        let _ = std::fs::remove_dir_all(&dir);
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
        crate::consolidate::serialise_binary_dump(&Engine::new(&view, &cache), &view, &dir)
            .unwrap();

        // Read it back: id → name / age, and the edges as (src-name, dst-name, reltype).
        use graph_format::consolidate_dump::DumpReader;
        let r = DumpReader::open(&dir).unwrap();
        let keys = r.meta().property_keys.clone();
        let reltypes = r.meta().reltypes.clone();
        let mut id_name: HashMap<u64, String> = HashMap::new();
        let mut id_age: HashMap<u64, i64> = HashMap::new();
        r.for_each_node(|id, _lb, pb| {
            for (k, v) in graph_format::columns::decode_props(pb).unwrap() {
                match keys[k as usize].as_str() {
                    "name" => {
                        if let graph_format::ids::Value::Str(s) = v {
                            id_name.insert(id, s);
                        }
                    }
                    "age" => {
                        if let graph_format::ids::Value::Int(i) = v {
                            id_age.insert(id, i);
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        })
        .unwrap();
        let mut edges: Vec<(String, String, String)> = Vec::new();
        r.for_each_edge(|_id, s, d, t, _pb| {
            edges.push((
                id_name[&s].clone(),
                id_name[&d].clone(),
                reltypes[t as usize].clone(),
            ));
            Ok(())
        })
        .unwrap();

        // Three survivors — Carol is gone, and the dense ids are gapless [0,1,2].
        assert_eq!(id_name.len(), 3, "Carol elided: Alice, Bob, Dave survive");
        let mut ids: Vec<u64> = id_name.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1, 2], "survivors renumbered gaplessly");
        let name_set: std::collections::HashSet<&str> =
            id_name.values().map(String::as_str).collect();
        assert!(
            !name_set.contains("Carol"),
            "the segment tombstone reclaimed Carol"
        );
        for expect in ["Alice", "Bob", "Dave"] {
            assert!(name_set.contains(expect), "{expect} present in the dump");
        }
        // The segment patch wins over the stale base bytes — THE fix under test.
        let age_of = |who: &str| -> i64 {
            let id = *id_name.iter().find(|(_, n)| n.as_str() == who).unwrap().0;
            id_age[&id]
        };
        assert_eq!(
            age_of("Alice"),
            99,
            "Alice carries the SEGMENT-patched age, not base 30"
        );
        assert_eq!(
            age_of("Bob"),
            25,
            "untouched base node keeps its byte-copied age"
        );
        assert_eq!(age_of("Dave"), 50, "segment-born node carried");

        // The surviving base edge and the born edge, both with compacted endpoints.
        assert_eq!(
            edges.len(),
            2,
            "Alice→Bob (base) + Dave→Bob (born): {edges:?}"
        );
        assert!(edges.contains(&("Alice".into(), "Bob".into(), "KNOWS".into())));
        assert!(edges.contains(&("Dave".into(), "Bob".into(), "KNOWS".into())));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 7 slice 7.1 (orchestration): `consolidate_graph` over a **stacked** set folds it
    /// back to a singleton via the Phase-0 direct dump path — the terminal D50 rung. The
    /// injected builder asserts the dump it is handed reflects the folded segment state (proving
    /// the retarget reads through the stack, not the stale base), then publishes an
    /// independently-correct singleton; afterwards the served core is a singleton (the stack
    /// collapsed), the writer is re-bound, and a post-freeze write is carried forward.
    #[test]
    fn consolidate_over_a_stacked_set_collapses_to_a_singleton() {
        let (root, _g) = testgen::write_indexed_people("retarget_e2e_71");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        // Flush a patch + delete + born into a segment, so the core we consolidate is stacked.
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
        write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");
        let gen0 = graphs.get("people").unwrap();
        assert_eq!(
            gen0.stack().segments().len(),
            1,
            "core is stacked before the retarget"
        );

        // Builder stand-in: assert the dump carries the folded segment state (Alice patched,
        // Carol gone, Dave born), apply a post-freeze write (Bob→77) modelling a client writing
        // during the rebuild, then publish an independently-correct singleton.
        let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0071);
        let writer = graphs.writer("people").unwrap();
        let writer_mid = writer.clone();
        let gen_mid = gen0.clone();
        let build = |dump: &Path, g: &str, dd: &Path| -> Result<()> {
            let nodes = dump_nodes(dump);
            assert_eq!(
                dump_age(dump, "Alice"),
                Some(99),
                "dump carries the segment patch"
            );
            assert!(
                !nodes.contains_key("Carol"),
                "dump reclaimed the segment tombstone"
            );
            assert_eq!(
                dump_age(dump, "Dave"),
                Some(50),
                "dump carries the segment-born node"
            );
            assert_eq!(g, "people");
            let bob = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
            execute_write(&writer_mid, gen_mid.as_ref(), &bob, &HashMap::new()).unwrap();
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            Ok(())
        };
        let published = graphs
            .consolidate_graph("people", &cache, &vc, &root, build)
            .unwrap();
        assert_eq!(
            published.0, new_uuid,
            "swapped to the consolidated singleton"
        );

        // The stack collapsed: the served core is now a singleton, the writer re-bound.
        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.uuid().0, new_uuid);
        assert!(
            gen1.stack().is_singleton(),
            "the retarget folded the segment stack into a singleton base"
        );
        assert_eq!(
            writer.core_uuid(),
            gen1.uuid(),
            "writer re-bound to the new core"
        );
        // The post-freeze write survived as a delta re-resolved onto the new core.
        let read_age = |name: &str| -> Val {
            let view = MergedView::new(gen1.as_ref(), writer.delta_snapshot());
            let ast =
                parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
            let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
            age
        };
        assert!(
            matches!(read_age("Bob"), Val::Int(77)),
            "post-freeze write carried forward"
        );
        assert!(!root.join("people").join(".consolidate.dump").exists());

        std::fs::remove_dir_all(&root).ok();
    }

    // ── Phase 7 slice 7.2: orphan segment/set GC ─────────────────────────────────

    /// The segment directory names (uuid dirs, skipping dot-files) under `<root>/people/`.
    fn seg_dirs(root: &Path) -> Vec<String> {
        std::fs::read_dir(root.join("people").join("segments"))
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| !n.starts_with('.'))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The `<uuid>.json` set manifest file names under `<root>/people/sets/`.
    fn set_files(root: &Path) -> Vec<String> {
        std::fs::read_dir(root.join("people").join("sets"))
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.ends_with(".json") && !n.starts_with('.'))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Phase 7 slice 7.2: the GC sweep reclaims the disk the flush and compaction slices
    /// intentionally leave behind. Two flushes stack two segments and orphan the first set;
    /// GC reclaims the stale set while both (live) segments survive. Compacting the two
    /// segments into one then orphans the run's two dirs + the pre-compaction set; GC reclaims
    /// exactly those, keeping the merged segment and the current set — and never touching the
    /// base generation directory. Reads stay consistent across the whole sweep.
    #[test]
    fn gc_reclaims_stale_sets_and_compacted_segments() {
        let (root, _g) = testgen::write_indexed_people("gc_reclaim_72");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().base_uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };

        // Two flushes → two segments; `current` names set2 (base + seg1 + seg2), set1 is stale.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 2);
        assert_eq!(set_files(&root).len(), 2, "set1 (stale) + set2 (current)");
        assert_eq!(seg_dirs(&root).len(), 2, "two live segments");

        // Immediate GC reclaims the stale set1.json; both segments are live under set2.
        let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert_eq!(rep.deleted_sets.len(), 1, "the stale set is reclaimed");
        assert!(
            rep.deleted_segments.is_empty(),
            "both segments live under set2"
        );
        assert_eq!(set_files(&root).len(), 1, "only the current set remains");
        assert_eq!(seg_dirs(&root).len(), 2, "segments untouched");

        // Compact the two segments into one → set3 (base + merged); seg1, seg2 and set2 orphan.
        graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();
        assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 1);
        assert_eq!(
            seg_dirs(&root).len(),
            3,
            "2 compacted + 1 merged on disk pre-GC"
        );

        let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert_eq!(
            rep.deleted_segments.len(),
            2,
            "the compacted run's dirs reclaimed"
        );
        assert_eq!(
            rep.deleted_sets.len(),
            1,
            "the pre-compaction set reclaimed"
        );
        assert_eq!(seg_dirs(&root).len(), 1, "only the merged segment remains");
        assert_eq!(set_files(&root).len(), 1, "only the current set remains");
        assert!(
            root.join("people").join(base_uuid.0.to_string()).exists(),
            "GC never touches the base generation directory"
        );

        // Reads are consistent after the sweep: 3 base + Dave + Eve.
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let n = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) RETURN count(*)").unwrap())
            .unwrap();
        assert!(matches!(n.rows[0][0], Val::Int(5)), "count intact after GC");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 7 slice 7.2: an orphan is not deleted until it has been observed unreferenced for
    /// the grace period. A stale set is marked (not deleted) by sweeps within the grace, and
    /// only an eligible (here: immediate) sweep reclaims it — the reader-safety guarantee.
    #[test]
    fn gc_respects_the_grace_before_reclaiming() {
        let (root, _g) = testgen::write_indexed_people("gc_grace_72");
        let wal = root.join("_wal");
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        // Two flushes orphan set1.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        assert_eq!(set_files(&root).len(), 2, "set1 stale + set2 current");

        // A large grace: the first sweep only *marks* the stale set — nothing is deleted.
        let rep = graphs.gc_orphan_segments("people", &root, 3600).unwrap();
        assert!(
            rep.deleted_sets.is_empty() && rep.deleted_segments.is_empty(),
            "nothing deleted within the grace"
        );
        assert!(
            rep.marked >= 1,
            "the stale set was marked for a later sweep"
        );
        assert_eq!(
            set_files(&root).len(),
            2,
            "stale set still present within grace"
        );
        // A second sweep, still within the grace, keeps waiting.
        let rep2 = graphs.gc_orphan_segments("people", &root, 3600).unwrap();
        assert!(rep2.deleted_sets.is_empty(), "still waiting out the grace");
        assert_eq!(set_files(&root).len(), 2);
        // Once eligible (immediate), the stale set is reclaimed.
        let rep3 = graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert_eq!(rep3.deleted_sets.len(), 1, "eligible orphan reclaimed");
        assert_eq!(set_files(&root).len(), 1, "only the current set remains");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 7 slice 7.2: after a retarget collapses a stacked set to a singleton (slice 7.1),
    /// `current` names a bare generation with no set file — so the *whole* prior set and every
    /// one of its segments is orphaned. GC reclaims them all, leaving the base generation and
    /// the freshly built singleton generation directories intact and the graph readable.
    #[test]
    fn gc_after_retarget_reclaims_the_prior_set() {
        let (root, _g) = testgen::write_indexed_people("gc_retarget_72");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().base_uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        // Flush a segment so the core is stacked (set1 over base + seg).
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        assert_eq!(seg_dirs(&root).len(), 1);
        assert_eq!(set_files(&root).len(), 1);

        // Retarget to a singleton via an injected builder that publishes a fresh generation.
        let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0072);
        let build = |_dump: &Path, g: &str, dd: &Path| -> Result<()> {
            assert_eq!(g, "people");
            testgen::write_indexed_people_at(dd, new_uuid, [30, 25, 40]);
            Ok(())
        };
        graphs
            .consolidate_graph("people", &cache, &vc, &root, build)
            .unwrap();
        let gen1 = graphs.get("people").unwrap();
        assert!(gen1.stack().is_singleton(), "retarget collapsed the stack");
        assert_eq!(gen1.uuid().0, new_uuid);
        // The prior set + segment linger on disk until GC (the deferred reclamation).
        assert_eq!(seg_dirs(&root).len(), 1, "prior segment lingers pre-GC");
        assert_eq!(set_files(&root).len(), 1, "prior set lingers pre-GC");

        // GC reclaims the whole prior set + its segment (current is a bare singleton gen).
        let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert_eq!(rep.deleted_segments.len(), 1, "prior segment reclaimed");
        assert_eq!(rep.deleted_sets.len(), 1, "prior set reclaimed");
        assert_eq!(seg_dirs(&root).len(), 0);
        assert_eq!(set_files(&root).len(), 0);
        // Both generation directories survive — GC only touches segments/ and sets/.
        assert!(
            root.join("people").join(base_uuid.0.to_string()).exists(),
            "base generation survives"
        );
        assert!(
            root.join("people").join(new_uuid.to_string()).exists(),
            "the retargeted singleton generation survives"
        );

        // The singleton still serves.
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let alice = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap())
            .unwrap();
        assert!(
            matches!(alice.rows[0][0], Val::Int(30)),
            "singleton readable after GC"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.1: a births-only delta folds into an upper core segment (the
    /// O(delta) T2 flush), the base is preserved, and every born entity reads back from
    /// the segment (index seek, count, traversal) with an empty delta — surviving a reopen.
    #[test]
    fn flush_to_segment_folds_births_into_a_core_segment() {
        let (root, _g) = testgen::write_indexed_people("flush_seg_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        write(
            &graphs,
            "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Eve'})",
        );

        // Flush the delta into an upper core segment.
        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");

        // The served generation is a new set over the *same* base, carrying one segment.
        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.uuid(), set_uuid, "identity is the new set uuid");
        assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
        assert_eq!(gen1.stack().segments().len(), 1, "one upper segment");

        // The delta is retired: the active memtable is empty, the writer is re-bound.
        let writer = graphs.writer("people").unwrap();
        assert!(writer.snapshot().is_empty(), "delta retired empty");
        assert_eq!(writer.core_uuid(), set_uuid, "writer re-bound to the set");

        // Read back with an empty delta — every born entity is served from the segment.
        let q = |graphs: &Graphs, q: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let r = Engine::new(&view, &cache).run(&ast).unwrap();
            r
        };
        // Index seek (name is indexed in the base) finds the flushed born node's props.
        let dave = q(
            &graphs,
            "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age",
        );
        assert_eq!(dave.rows.len(), 1, "index seek finds Dave in the segment");
        assert!(
            matches!(dave.rows[0][1], Val::Int(50)),
            "Dave age from segment"
        );
        // Count over the merged marginals: 3 base + 2 born.
        let n = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n.rows[0][0], Val::Int(5)),
            "3 base + 2 born from the segment: {:?}",
            n.rows[0][0]
        );
        // The born edge traverses from the segment adjacency.
        let knows = q(
            &graphs,
            "MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name",
        );
        assert_eq!(knows.rows.len(), 1, "the born KNOWS edge traverses");
        assert!(
            matches!(&knows.rows[0][0], Val::Str(s) if s == "Eve"),
            "KNOWS target from segment: {:?}",
            knows.rows[0][0]
        );

        // Reopen from disk: the set + segment reload, and the data survives.
        drop(writer);
        drop(gen1);
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        let gen2 = graphs.get("people").unwrap();
        assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
        assert_eq!(gen2.stack().segments().len(), 1, "segment reloaded");
        let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
        let ast = parser::parse("MATCH (n:Person {name:'Eve'}) RETURN n.age").unwrap();
        let eve = Engine::new(&view, &cache).run(&ast).unwrap();
        assert!(
            matches!(eve.rows[0][0], Val::Int(60)),
            "Eve reloaded from the segment: {:?}",
            eve.rows[0][0]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 6 slice 6.1: the write path resolves a business key **through the core stack**,
    /// closing the 4.1 note (e) gap. After a flush moves born nodes into a segment, a
    /// re-`MERGE` of one of those keys must resolve to the *segment* id — patching it in place
    /// — rather than allocate a duplicate born node; a `MERGE` of a base key still resolves to
    /// the base id; and an edge whose endpoint is a **segment-born** node resolves that
    /// endpoint through the fold too. A second flush folds the patches/born edge into a second
    /// segment and the counts are still duplicate-free after a reopen.
    #[test]
    fn resolve_through_the_stack_reuses_a_flushed_key_no_duplicate() {
        let (root, _g) = testgen::write_indexed_people("resolve_stack_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        let q = |graphs: &Graphs, q: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let r = Engine::new(&view, &cache).run(&ast).unwrap();
            r
        };

        // Flush two born nodes + a born edge into an upper segment.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        write(
            &graphs,
            "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Eve'})",
        );
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");
        assert!(
            graphs.writer("people").unwrap().snapshot().is_empty(),
            "delta retired empty after the flush"
        );

        // Re-MERGE the *segment-born* key Dave: it must resolve to the segment id and patch it,
        // NOT create a second Dave. Without the stack fold, resolve returns Absent → duplicate.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 99");
        // MERGE a *base* key: resolves to the base id and patches it.
        write(&graphs, "MERGE (n:Person {name:'Alice'}) SET n.age = 31");
        // An edge whose source endpoint is the segment-born Dave resolves that endpoint through
        // the fold (via resolve_endpoint → resolve_business_key), and the base Carol as dst.
        write(
            &graphs,
            "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(c:Person {name:'Carol'})",
        );

        // Exactly one Dave, patched to 99 (the delta patch over the segment row).
        let dave = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
        assert_eq!(
            dave.rows.len(),
            1,
            "exactly one Dave — no duplicate born node"
        );
        assert!(
            matches!(dave.rows[0][0], Val::Int(99)),
            "Dave patched to 99"
        );
        // Alice patched over the base row; still one Alice.
        let alice = q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age");
        assert_eq!(alice.rows.len(), 1, "exactly one Alice");
        assert!(
            matches!(alice.rows[0][0], Val::Int(31)),
            "Alice patched to 31"
        );
        // 3 base + 2 born = 5 people, no duplicates introduced by the re-MERGEs.
        let n = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n.rows[0][0], Val::Int(5)),
            "5 people: {:?}",
            n.rows[0][0]
        );
        // Dave now KNOWS both Eve (segment edge) and Carol (the new born edge over a folded
        // segment endpoint).
        let mut targets: Vec<String> = q(
            &graphs,
            "MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name",
        )
        .rows
        .into_iter()
        .map(|r| match &r[0] {
            Val::Str(s) => s.clone(),
            other => panic!("expected a name: {other:?}"),
        })
        .collect();
        targets.sort();
        assert_eq!(
            targets,
            vec!["Carol".to_string(), "Eve".to_string()],
            "Dave KNOWS Eve + Carol"
        );

        // A second flush folds the patches + the new born edge into a second segment; the id
        // space and counts are unchanged (the re-MERGEs never duplicated).
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("the second delta flushes");
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            2,
            "two upper segments after the second flush"
        );
        let n2 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n2.rows[0][0], Val::Int(5)),
            "still 5 after the second flush"
        );
        let dave2 = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
        assert_eq!(dave2.rows.len(), 1, "still one Dave");
        assert!(
            matches!(dave2.rows[0][0], Val::Int(99)),
            "Dave 99 folded into seg 2"
        );

        // Reopen from disk: the two-segment set reloads and resolution still de-duplicates.
        drop(graphs);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let n3 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n3.rows[0][0], Val::Int(5)),
            "5 after reopen: {:?}",
            n3.rows[0][0]
        );
        // A re-MERGE of Dave after the reopen still resolves through the reloaded stack.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 77");
        let dave3 = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
        assert_eq!(
            dave3.rows.len(),
            1,
            "still one Dave after reopen + re-MERGE"
        );
        assert!(
            matches!(dave3.rows[0][0], Val::Int(77)),
            "Dave re-patched to 77 post-reopen"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 6 slice 6.3: the **batched** write path (`execute_write_batch`) resolves the whole
    /// batch's business keys through the core stack in one merge-join sweep
    /// (`resolve_business_keys_batch`) — byte-identically to the per-row single path, but at one
    /// block decompress per touched fragment block instead of per row (the bulk-write ISAM
    /// floor, memory `bulk-delete-isam-resolve-floor`). A single `UNWIND … MERGE … SET` batch
    /// over a flushed segment must: reuse a *segment-born* key (patch, no duplicate), patch a
    /// *base* key, born an *absent* key, and honour a *within-batch duplicate* key (both rows
    /// resolve to the same id, group-commit LWW) — leaving the graph duplicate-free.
    #[test]
    fn batch_resolve_through_the_stack_reuses_flushed_keys_no_duplicate() {
        let (root, _g) = testgen::write_indexed_people("batch_resolve_stack_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        let batch = |graphs: &Graphs, q: &str, params: &HashMap<String, Val>| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, params).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        let ages = |graphs: &Graphs, nm: &str| -> Vec<i64> {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let qy = format!("MATCH (n:Person {{name:'{nm}'}}) RETURN n.age");
            let res = Engine::new(&view, &cache)
                .run(&parser::parse(&qy).unwrap())
                .unwrap();
            res.rows
                .iter()
                .filter_map(|r| match &r[0] {
                    Val::Int(n) => Some(*n),
                    _ => None,
                })
                .collect()
        };
        let count = |graphs: &Graphs| -> i64 {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let res = Engine::new(&view, &cache)
                .run(&parser::parse("MATCH (n:Person) RETURN count(*)").unwrap())
                .unwrap();
            match res.rows[0][0] {
                Val::Int(n) => n,
                ref v => panic!("count not int: {v:?}"),
            }
        };

        // Flush two born nodes into an upper segment (Dave, Eve).
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");
        assert_eq!(count(&graphs), 5, "3 base + 2 flushed born");

        // One batch: Dave (segment-born → patch), Alice (base → patch), Frank (absent → born),
        // Dave again (within-batch duplicate → same id, group-commit LWW). The merge-join
        // resolve must fold the stack for every distinct key in the sweep.
        let rows = Val::List(vec![
            Val::Map(vec![
                ("name".into(), Val::Str("Dave".into())),
                ("age".into(), Val::Int(99)),
            ]),
            Val::Map(vec![
                ("name".into(), Val::Str("Alice".into())),
                ("age".into(), Val::Int(31)),
            ]),
            Val::Map(vec![
                ("name".into(), Val::Str("Frank".into())),
                ("age".into(), Val::Int(40)),
            ]),
            Val::Map(vec![
                ("name".into(), Val::Str("Dave".into())),
                ("age".into(), Val::Int(88)),
            ]),
        ]);
        let mut params = HashMap::new();
        params.insert("rows".to_string(), rows);
        batch(
            &graphs,
            "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
            &params,
        );

        // Duplicate-free: one Dave (LWW → 88), one Alice (patched → 31), one born Frank (40).
        assert_eq!(ages(&graphs, "Dave"), vec![88], "one Dave, last write wins");
        assert_eq!(ages(&graphs, "Alice"), vec![31], "base Alice patched once");
        assert_eq!(ages(&graphs, "Frank"), vec![40], "absent Frank born once");
        assert_eq!(count(&graphs), 6, "5 + 1 born Frank, no duplicates");

        // Flush + reopen: the batch resolve still de-duplicates against the reloaded 2-seg set.
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("the second delta flushes");
        drop(graphs);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        assert_eq!(count(&graphs), 6, "6 after reopen");

        // A second batch re-touching the now-flushed Dave/Frank keys reuses them (no dup).
        let rows2 = Val::List(vec![
            Val::Map(vec![
                ("name".into(), Val::Str("Dave".into())),
                ("age".into(), Val::Int(77)),
            ]),
            Val::Map(vec![
                ("name".into(), Val::Str("Frank".into())),
                ("age".into(), Val::Int(41)),
            ]),
        ]);
        let mut params2 = HashMap::new();
        params2.insert("rows".to_string(), rows2);
        batch(
            &graphs,
            "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
            &params2,
        );
        assert_eq!(
            ages(&graphs, "Dave"),
            vec![77],
            "Dave re-patched post-reopen"
        );
        assert_eq!(
            ages(&graphs, "Frank"),
            vec![41],
            "Frank re-patched post-reopen"
        );
        assert_eq!(count(&graphs), 6, "still 6 — batch reuse, no duplicate");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 6 slice 6.1: a base key **deleted into a segment** resolves `Absent` on the write
    /// path (its base index entry is superseded by the segment's `removals` sidecar, folded by
    /// `CoreStack::fold_index_eq`), so a re-`MERGE` **reborns** it as a fresh born node rather
    /// than resurrecting the tombstoned id — and a second re-`MERGE` is idempotent (the born
    /// node resolves through the memtable's own identity, not the stack).
    #[test]
    fn resolve_reborns_a_key_deleted_into_a_segment() {
        let (root, _g) = testgen::write_indexed_people("resolve_rebirth_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a node write: {q}"),
            }
        };
        let q = |graphs: &Graphs, q: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let r = Engine::new(&view, &cache).run(&ast).unwrap();
            r
        };

        // Delete a base node with no incident edges (Carol — the only base edge is Alice→Bob),
        // then flush the tombstone into a segment.
        write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("the delete flushes");
        assert!(graphs.writer("people").unwrap().snapshot().is_empty());
        let n0 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n0.rows[0][0], Val::Int(2)),
            "Carol gone: 2 people left"
        );
        let gone = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
        assert_eq!(
            gone.rows.len(),
            0,
            "Carol resolves to nothing after the delete flush"
        );

        // MERGE Carol: resolve returns Absent (the segment removals suppress her base entry),
        // so she is reborn as a fresh born node — count climbs back to 3.
        write(&graphs, "MERGE (n:Person {name:'Carol'}) SET n.age = 41");
        let n1 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n1.rows[0][0], Val::Int(3)),
            "Carol reborn: 3 people"
        );
        let carol = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
        assert_eq!(carol.rows.len(), 1, "exactly one (reborn) Carol");
        assert!(
            matches!(carol.rows[0][0], Val::Int(41)),
            "reborn Carol's age"
        );

        // A second MERGE is idempotent — the born Carol resolves through the memtable, not the
        // stack (which still says Absent), so no fourth node appears.
        write(&graphs, "MERGE (n:Person {name:'Carol'}) SET n.age = 42");
        let n2 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n2.rows[0][0], Val::Int(3)),
            "re-MERGE idempotent: still 3"
        );
        let carol2 = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
        assert_eq!(carol2.rows.len(), 1, "still one Carol");
        assert!(
            matches!(carol2.rows[0][0], Val::Int(42)),
            "the born Carol re-patched"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.4-b: **encryption parity**. When the served core is encrypted at rest,
    /// a flush must write an encrypted segment — the writer derives a fresh per-segment cipher
    /// and KDF header, stamps `manifest.encryption`, and seals the MAC. The segment reopens
    /// (MAC-verified, sections decrypted) *with* the key and its born data reads back through
    /// an empty delta; reopening the same data directory *without* the key is refused.
    #[test]
    fn flush_to_segment_encrypts_the_segment_under_a_master_key() {
        let key: &[u8] = b"an-at-rest-master-key-32byteslong";
        let (root, _g) = testgen::write_indexed_people_keyed("flush_seg_keyed_e2e", Some(key));
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, Some(key)).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(
            &graphs,
            "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Alice'})",
        );

        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");

        // The new segment carries its own encryption header (salt only) — proof the flush
        // wrote ciphertext, not plaintext beside the encrypted core.
        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
        let seg = &gen1.stack().segments()[0];
        let header = seg
            .manifest
            .encryption
            .as_ref()
            .expect("flushed segment manifest carries an encryption header");
        assert_eq!(header.aead, graph_format::crypto::AEAD_NAME);
        assert!(
            seg.manifest.mac.is_some(),
            "flushed segment manifest is MAC-sealed"
        );

        // Read back with an empty delta (still keyed): the born, encrypted node decrypts.
        let dave = {
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen1.as_ref(), w.delta_snapshot());
            let ast = parser::parse("MATCH (n:Person {name:'Dave'}) RETURN n.age").unwrap();
            let r = Engine::new(&view, &cache).run(&ast).unwrap();
            r
        };
        assert!(
            matches!(dave.rows[0][0], Val::Int(50)),
            "Dave decrypts from the keyed segment: {:?}",
            dave.rows[0][0]
        );
        drop(gen1);

        // Reopen the whole data dir WITH the key — set + encrypted segment reload and verify.
        drop(graphs);
        let graphs = Graphs::open_all(&root, Some(key)).unwrap();
        let gen2 = graphs.get("people").unwrap();
        assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
        let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
        let ast =
            parser::parse("MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name").unwrap();
        let knows = Engine::new(&view, &cache).run(&ast).unwrap();
        assert!(
            matches!(&knows.rows[0][0], Val::Str(s) if s == "Alice"),
            "the born encrypted edge traverses after reopen: {:?}",
            knows.rows.first()
        );
        drop(gen2);
        drop(graphs);

        // Reopen WITHOUT the key — the encrypted base + segment are refused (no plaintext leak).
        assert!(
            Graphs::open_all(&root, None).is_err(),
            "an encrypted data dir must not open without the key"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.4-c: a flush over a **stacked L0** (the active memtable plus ≥2 sealed
    /// L0 levels) folds every level newest-wins into ONE segment. A core node patched in all
    /// three levels resolves to the newest value; born nodes allocated in different levels tile
    /// contiguously above the shared base; a born edge whose endpoints span levels traverses.
    /// All read back through an empty delta and survive a reopen.
    #[test]
    fn flush_to_segment_folds_a_stacked_l0() {
        let (root, _g) = testgen::write_indexed_people("flush_seg_stacked_l0");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };

        // Level L0-oldest: patch a core node only (0 born).
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
        assert!(graphs.writer("people").unwrap().flush_to_l0().unwrap());

        // Level L0-newer: re-patch the same core node (newer wins over 99), born Dave, and a
        // born edge Alice-KNOWS->Dave (a core endpoint + a same-level born endpoint).
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 77");
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(
            &graphs,
            "MERGE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Dave'})",
        );
        assert!(graphs.writer("people").unwrap().flush_to_l0().unwrap());
        assert_eq!(
            graphs.writer("people").unwrap().l0_len(),
            2,
            "two L0 levels"
        );

        // Active memtable (newest): re-patch Alice again (55 wins over 77 and 99), born Eve.
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 55");
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        assert!(
            !graphs.writer("people").unwrap().snapshot().is_empty(),
            "active memtable carries the newest level"
        );

        // Flush: folds [active ⊕ L0-newer ⊕ L0-oldest] into one segment.
        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a stacked delta flushes");

        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.uuid(), set_uuid, "identity is the new set uuid");
        assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
        assert_eq!(gen1.stack().segments().len(), 1, "one folded upper segment");
        let writer = graphs.writer("people").unwrap();
        assert!(writer.snapshot().is_empty(), "delta retired empty");
        assert_eq!(writer.l0_len(), 0, "L0 levels consumed by the flush");

        let q = |graphs: &Graphs, q: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let r = Engine::new(&view, &cache).run(&ast).unwrap();
            r
        };

        // Newest-wins across three levels: Alice's age is 55 (active), not 77 or 99.
        let alice = q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age");
        assert!(
            matches!(alice.rows[0][0], Val::Int(55)),
            "Alice's newest patch wins across the stack: {:?}",
            alice.rows[0][0]
        );
        // Born nodes from different levels both land (Dave from L0-newer, Eve from active).
        let dave = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
        assert!(
            matches!(dave.rows[0][0], Val::Int(50)),
            "Dave (born in a sealed L0) is in the segment: {:?}",
            dave.rows[0][0]
        );
        let eve = q(&graphs, "MATCH (n:Person {name:'Eve'}) RETURN n.age");
        assert!(
            matches!(eve.rows[0][0], Val::Int(60)),
            "Eve (born in the active level) is in the segment: {:?}",
            eve.rows[0][0]
        );
        // Count: 3 base + 2 born = 5.
        let n = q(&graphs, "MATCH (n:Person) RETURN count(*)");
        assert!(
            matches!(n.rows[0][0], Val::Int(5)),
            "3 base + 2 born folded: {:?}",
            n.rows[0][0]
        );
        // The born edge (endpoints resolved across levels) traverses.
        let knows = q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name ORDER BY b.name",
        );
        // Alice already KNOWS Bob in the base; the folded born edge adds Dave.
        let targets: Vec<String> = knows
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(
            targets.contains(&"Dave".to_string()),
            "the folded born edge Alice->Dave traverses: {targets:?}"
        );

        // Reopen from disk: the folded segment reloads and the merged data survives.
        drop(writer);
        drop(gen1);
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        let gen2 = graphs.get("people").unwrap();
        assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
        assert_eq!(gen2.stack().segments().len(), 1, "folded segment reloaded");
        let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
        let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
        let alice2 = Engine::new(&view, &cache).run(&ast).unwrap();
        assert!(
            matches!(alice2.rows[0][0], Val::Int(55)),
            "newest-wins fold survives reopen: {:?}",
            alice2.rows[0][0]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Recursively load every file under `root` into a `MemObjectStore`, keyed by its
    /// `/`-joined path relative to `root` — the same keys the store abstraction builds.
    fn load_dir_into_mem(
        store: &graph_format::store::mem::MemObjectStore,
        root: &Path,
        dir: &Path,
    ) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                load_dir_into_mem(store, root, &path);
            } else {
                let key = path
                    .strip_prefix(root)
                    .unwrap()
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                store
                    .put(&key, &std::fs::read(&path).unwrap(), None)
                    .unwrap();
            }
        }
    }

    /// Phase 4 slice 4.4-d: a flush against a **non-filesystem** store uploads the segment,
    /// set manifest and `current` pointer through the `ObjectStore` abstraction (the segment
    /// is staged locally, then published to the store). A fresh open that reads *only* through
    /// the in-memory store — no local filesystem — serves the flushed born node, proving the
    /// upload round-trips store-natively.
    #[test]
    fn flush_to_segment_uploads_to_an_object_store() {
        use graph_format::store::mem::MemObjectStore;
        use graph_format::store::ObjectStore as _;

        // Build the base generation locally, then seed a mem store from it — the mem store is
        // the served backend; the local dir is only the WAL + segment staging area.
        let (root, _g) = testgen::write_indexed_people("flush_seg_memstore");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mem = Arc::new(MemObjectStore::new());
        load_dir_into_mem(&mem, &root, &root);

        let mut graphs =
            Graphs::open_all_with_store(mem.clone() as Arc<dyn ObjectStore>, None, true, None)
                .unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a node write: {q}"),
            }
        };
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");

        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");

        // The store now holds the set, an updated `current`, and the segment's SEGMENT.json.
        assert_eq!(
            String::from_utf8(mem.read_all("people/current").unwrap())
                .unwrap()
                .trim(),
            set_uuid.0.to_string(),
            "remote current names the flushed set"
        );
        assert!(
            mem.exists(&graph_format::setmanifest::SetManifest::key(
                "people", set_uuid
            ))
            .unwrap(),
            "the set manifest was uploaded"
        );
        let seg_json_keys: Vec<String> = mem
            .list("people/segments")
            .unwrap()
            .iter()
            .map(|u| format!("people/segments/{u}/SEGMENT.json"))
            .collect();
        assert_eq!(seg_json_keys.len(), 1, "one segment dir uploaded");
        assert!(
            mem.exists(&seg_json_keys[0]).unwrap(),
            "SEGMENT.json uploaded to the store"
        );

        // Reopen reading ONLY through the mem store (no local fs): the flushed data is served.
        drop(graphs);
        let graphs =
            Graphs::open_all_with_store(mem.clone() as Arc<dyn ObjectStore>, None, true, None)
                .unwrap();
        let gen = graphs.get("people").unwrap();
        assert_eq!(gen.uuid(), set_uuid, "store reopen names the flushed set");
        assert_eq!(gen.base_uuid(), base_uuid, "base preserved");
        assert_eq!(gen.stack().segments().len(), 1, "segment loaded from store");
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
        let ast = parser::parse("MATCH (n:Person {name:'Dave'}) RETURN n.age").unwrap();
        let dave = Engine::new(&view, &cache).run(&ast).unwrap();
        assert!(
            matches!(dave.rows[0][0], Val::Int(50)),
            "born Dave served from the store-native segment: {:?}",
            dave.rows.first()
        );
        let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
        let alice = Engine::new(&view, &cache).run(&ast).unwrap();
        assert!(
            matches!(alice.rows[0][0], Val::Int(99)),
            "Alice's flushed patch served from the store: {:?}",
            alice.rows.first()
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 7 slice 7.4: GC reclaims a **remote** store's orphaned objects, not only local
    /// staged dirs. Over a `MemObjectStore` (`is_local_fs == false`), a stale set's manifest and
    /// a compacted run's segment objects are removed from the store via `ObjectStore::delete`; a
    /// store-native reopen then serves only the live merged segment.
    #[test]
    fn gc_reclaims_orphans_from_an_object_store() {
        use graph_format::store::mem::MemObjectStore;
        use graph_format::store::ObjectStore as _;

        let (root, _g) = testgen::write_indexed_people("gc_memstore_74");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mem = Arc::new(MemObjectStore::new());
        load_dir_into_mem(&mem, &root, &root);

        let mut graphs =
            Graphs::open_all_with_store(mem.clone() as Arc<dyn ObjectStore>, None, true, None)
                .unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a node write: {q}"),
            }
        };
        // Number of segment "dirs" and set manifest objects the store currently holds.
        let store_segments =
            |mem: &MemObjectStore| -> usize { mem.list("people/segments").unwrap().len() };
        let store_sets = |mem: &MemObjectStore| -> usize {
            mem.list("people/sets")
                .unwrap()
                .into_iter()
                .filter(|n| n.ends_with(".json"))
                .count()
        };
        let set_key = |u: GenId| graph_format::setmanifest::SetManifest::key("people", u);

        // Two flushes upload two segments; set1 is now stale, set2 current.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        let set1 = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        assert_eq!(store_segments(&mem), 2, "two segments uploaded");
        assert_eq!(
            store_sets(&mem),
            2,
            "set1 (stale) + set2 (current) uploaded"
        );
        assert!(mem.exists(&set_key(set1)).unwrap(), "set1 object present");

        // GC reclaims the stale set1 manifest FROM THE STORE (not just a local file).
        let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert_eq!(rep.deleted_sets.len(), 1);
        assert!(
            !mem.exists(&set_key(set1)).unwrap(),
            "the stale set object was deleted from the store"
        );
        assert_eq!(store_sets(&mem), 1, "only the current set object remains");
        assert_eq!(store_segments(&mem), 2, "both segments still live");

        // Compact the two segments into one → the run's two segments orphan in the store.
        graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();
        assert_eq!(
            store_segments(&mem),
            3,
            "2 compacted + 1 merged in the store pre-GC"
        );

        let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert_eq!(
            rep.deleted_segments.len(),
            2,
            "the run's segment objects reclaimed from the store"
        );
        assert_eq!(
            rep.deleted_sets.len(),
            1,
            "the superseded set object reclaimed"
        );
        assert_eq!(
            store_segments(&mem),
            1,
            "only the merged segment remains in the store"
        );
        assert_eq!(store_sets(&mem), 1);

        // The merged segment's objects are intact — a store-native reopen serves every row.
        drop(graphs);
        let graphs =
            Graphs::open_all_with_store(mem.clone() as Arc<dyn ObjectStore>, None, true, None)
                .unwrap();
        let gen = graphs.get("people").unwrap();
        assert_eq!(
            gen.stack().segments().len(),
            1,
            "merged segment loads from the store"
        );
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
        let names: HashSet<String> = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) RETURN n.name").unwrap())
            .unwrap()
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        for n in ["Alice", "Bob", "Carol", "Dave", "Eve"] {
            assert!(names.contains(n), "{n} served after store GC: {names:?}");
        }

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.2: a delta of **core-resolved node patches** (a `SET`/`REMOVE` on a
    /// node the base already carries) flushes into an upper segment as full replace-rows.
    /// Every kind is exercised end-to-end through the query overlay with an empty delta:
    /// a moved indexed value (base index entry superseded via the removal sidecar + the new
    /// value re-added), a removed indexed value, a fresh non-indexed property (base props
    /// preserved in the full row), an added label, and a mixed-in born node — all surviving
    /// a reopen.
    #[test]
    fn flush_to_segment_materialises_core_node_patches() {
        // `write_basic` gives Alice/Bob/Carol :Person (name+age indexed, ages 30/25/40) and
        // Acme/Globex :Company, with both labels defined so a label-add is accepted.
        let (root, _g, _u) = testgen::write_basic("flush_seg_patch_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        // Alice(30) → 99 and gains the pre-existing :Company label; Bob gains a fresh
        // non-indexed city; Carol loses her indexed age; Zoe is a mixed-in birth.
        write(
            &graphs,
            "MATCH (n:Person {name:'Alice'}) SET n.age = 99, n:Company",
        );
        write(
            &graphs,
            "MATCH (n:Person {name:'Bob'}) SET n.city = 'Berlin'",
        );
        write(&graphs, "MATCH (n:Person {name:'Carol'}) REMOVE n.age");
        write(&graphs, "MERGE (n:Person {name:'Zoe'}) SET n.age = 7");

        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes");

        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.stack().segments().len(), 1, "one upper segment");
        assert!(
            graphs.writer("people").unwrap().snapshot().is_empty(),
            "delta retired empty"
        );

        // Query the flushed set with an empty delta — everything is served by the segment.
        let q = |graphs: &Graphs, q: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(q).unwrap())
                .unwrap();
            r
        };
        let names = |r: &QueryResult| -> Vec<String> {
            let mut ns: Vec<String> = r
                .rows
                .iter()
                .map(|row| match &row[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("expected a name string, got {v:?}"),
                })
                .collect();
            ns.sort();
            ns
        };

        // Moved indexed value: the old value is gone (removal sidecar suppressed the base
        // hit), the new value finds Alice, an untouched value still finds Bob.
        assert!(
            q(&graphs, "MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
                .rows
                .is_empty(),
            "Alice's old indexed age (30) is superseded"
        );
        assert_eq!(
            names(&q(
                &graphs,
                "MATCH (n:Person) WHERE n.age = 99 RETURN n.name"
            )),
            vec!["Alice"],
            "the moved indexed value finds Alice at 99"
        );
        assert_eq!(
            names(&q(
                &graphs,
                "MATCH (n:Person) WHERE n.age = 25 RETURN n.name"
            )),
            vec!["Bob"],
            "an untouched base index entry still stands"
        );
        // Removed indexed value: Carol's age index entry is gone, and her property reads Null
        // while her preserved base name survives in the full row.
        assert!(
            q(&graphs, "MATCH (n:Person) WHERE n.age = 40 RETURN n.name")
                .rows
                .is_empty(),
            "Carol's removed indexed age is superseded with no replacement"
        );
        let carol = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
        assert!(
            matches!(carol.rows[0][0], Val::Null),
            "Carol's age is removed: {:?}",
            carol.rows[0][0]
        );
        // Fresh non-indexed property with base props preserved.
        let bob = q(
            &graphs,
            "MATCH (n:Person {name:'Bob'}) RETURN n.city, n.age",
        );
        assert!(
            matches!(&bob.rows[0][0], Val::Str(s) if s == "Berlin"),
            "Bob's new city: {:?}",
            bob.rows[0][0]
        );
        assert!(
            matches!(bob.rows[0][1], Val::Int(25)),
            "Bob's base age preserved in the full row: {:?}",
            bob.rows[0][1]
        );
        // Added label surfaces in a label scan (Alice joins the base Companies); she is still
        // a Person too (the base label is preserved in the full row).
        assert_eq!(
            names(&q(&graphs, "MATCH (n:Company) RETURN n.name")),
            vec!["Acme", "Alice", "Globex"],
            "the added :Company label is served by the segment beside the base companies"
        );
        assert_eq!(
            names(&q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.name")),
            vec!["Alice"],
            "Alice keeps her base :Person label"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(4)
            ),
            "3 base Persons + born Zoe; patches do not change the node count"
        );
        // The mixed-in born node reads back through its index entry.
        assert_eq!(
            names(&q(
                &graphs,
                "MATCH (n:Person) WHERE n.age = 7 RETURN n.name"
            )),
            vec!["Zoe"],
            "the born node is found by its index entry"
        );

        // Reopen from disk: the patch full-rows and removal sidecars reload.
        drop(gen1);
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        let gen2 = graphs.get("people").unwrap();
        assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
        let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
        let alice = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap())
            .unwrap();
        assert!(
            matches!(alice.rows[0][0], Val::Int(99)),
            "Alice's patched age reloaded from the segment: {:?}",
            alice.rows[0][0]
        );
        assert!(
            Engine::new(&view, &cache)
                .run(&parser::parse("MATCH (n:Person) WHERE n.age = 30 RETURN n.name").unwrap())
                .unwrap()
                .rows
                .is_empty(),
            "the removal sidecar survives the reopen"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.2, cross-layer removal obligation: a second flush that re-patches a
    /// node already carried by the *first* flush's segment must supersede the value that
    /// lives in the **lower segment** (not just the base). The writer reads the base-below
    /// row through the stack, so it lists the lower segment's id in its removal sidecar, and
    /// the oldest→newest `fold_index_eq` yields newest-wins across two stacked segments.
    #[test]
    fn flush_to_segment_supersedes_a_lower_segment_value() {
        let (root, _g, _u) = testgen::write_basic("flush_seg_restack_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            let parser::ast::Statement::Write(w) = parser::parse_statement(qy).unwrap() else {
                panic!("expected a write: {qy}");
            };
            execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        // First flush: Alice 30 → 99 lands in segment #1.
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("first flush");
        // Second flush: Alice 99 → 7. The base-below value (99) lives in segment #1.
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 7");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("second flush");

        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            2,
            "two stacked segments"
        );
        // Newest value wins; both older values (the base's 30 and segment #1's 99) are gone.
        assert_eq!(
            q(&graphs, "MATCH (n:Person) WHERE n.age = 7 RETURN n.name")
                .rows
                .len(),
            1,
            "the newest flush's value wins across two segments"
        );
        assert!(
            q(&graphs, "MATCH (n:Person) WHERE n.age = 99 RETURN n.name")
                .rows
                .is_empty(),
            "segment #1's superseded value is dropped by segment #2's removal"
        );
        assert!(
            q(&graphs, "MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
                .rows
                .is_empty(),
            "the original base value stays superseded"
        );
        let alice = q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age");
        assert!(
            matches!(alice.rows[0][0], Val::Int(7)),
            "Alice's twice-patched age: {:?}",
            alice.rows[0][0]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.3: a **node delete** flushes into an upper segment as a full-row
    /// tombstone plus incident-edge removal fragments. `DETACH DELETE` of Bob (the target of
    /// the base's one Alice-KNOWS->Bob edge) must, once flushed with an empty delta: drop Bob
    /// from an index seek and the label count (its base-indexed values superseded via the
    /// `removals` sidecar, the node/label marginals netted down), and drop the incident edge
    /// from Alice's outgoing traversal and the reltype count (a `removed` adjacency fragment
    /// on Alice's surviving side, the edge marginal netted down) — all surviving a reopen.
    #[test]
    fn flush_to_segment_materialises_a_node_delete() {
        let (root, _g) = testgen::write_indexed_people("flush_seg_del_node_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        // DETACH DELETE Bob (dst of the Alice-KNOWS->Bob base edge), then flush.
        write(&graphs, "MATCH (n:Person {name:'Bob'}) DETACH DELETE n");
        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("a delete flushes a non-empty delta");

        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
        assert_eq!(gen1.stack().segments().len(), 1, "one upper segment");
        assert!(
            graphs.writer("people").unwrap().snapshot().is_empty(),
            "delta retired"
        );

        // Bob is gone from the index seek, the label count, and Alice's traversal — read
        // through the (now empty) delta, so the segment alone must answer.
        assert!(
            q(&graphs, "MATCH (n:Person {name:'Bob'}) RETURN n.name")
                .rows
                .is_empty(),
            "deleted Bob is superseded in the name index"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(2)
            ),
            "2 survivors (Alice, Carol) after the delete"
        );
        assert!(
            q(
                &graphs,
                "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
            )
            .rows
            .is_empty(),
            "the incident edge is removed on Alice's surviving side"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0],
                Val::Int(0)
            ),
            "the reltype edge count nets the removed edge to zero"
        );
        // Alice and Carol still read normally.
        assert_eq!(
            q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.name")
                .rows
                .len(),
            1,
            "Alice untouched by Bob's delete"
        );

        // Reopen from disk: the tombstone + removals reload and still hide Bob and his edge.
        drop(gen1);
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the set"
        );
        assert!(
            q(&graphs, "MATCH (n:Person {name:'Bob'}) RETURN n.name")
                .rows
                .is_empty(),
            "Bob stays deleted across a reopen"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(2)
            ),
            "survivor count stable across a reopen"
        );
        assert!(
            q(
                &graphs,
                "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
            )
            .rows
            .is_empty(),
            "the removed edge stays gone across a reopen"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.3: an explicit **edge delete** (`DELETE r` on a core edge, both
    /// endpoints surviving) flushes into an upper segment as a pure adjacency removal on
    /// *both* endpoints' sides (no node tombstone, no edge row) with the edge/reltype
    /// marginals netted down. The edge stops traversing from either direction while both
    /// nodes remain, surviving a reopen.
    #[test]
    fn flush_to_segment_materialises_an_edge_delete() {
        let (root, _g) = testgen::write_indexed_people("flush_seg_del_edge_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        write(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
        );
        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("an edge delete flushes a non-empty delta");

        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            1,
            "one upper segment"
        );
        // Both nodes remain; only the edge is gone, from both traversal directions.
        assert!(
            matches!(
                q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(3)
            ),
            "an edge delete leaves every node"
        );
        assert!(
            q(
                &graphs,
                "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
            )
            .rows
            .is_empty(),
            "removed on Alice's outgoing side"
        );
        assert!(
            q(
                &graphs,
                "MATCH (a)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name"
            )
            .rows
            .is_empty(),
            "removed on Bob's incoming side"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0],
                Val::Int(0)
            ),
            "the reltype edge count nets to zero"
        );

        // Reopen: the removal fragments reload and the edge stays gone.
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the set"
        );
        assert!(
            q(
                &graphs,
                "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
            )
            .rows
            .is_empty(),
            "the removed edge stays gone across a reopen"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(3)
            ),
            "node count stable across a reopen"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4 slice 4.4-a: a **core-edge patch** (`SET r.p = v` on an edge the core already
    /// carries) flushes into an upper segment as a full **replace** edge row — the base props
    /// overlaid by the patch — that `resolve_edge_row` serves over the base, with no marginal
    /// change (topology untouched). The base fixture's one edge `Alice-KNOWS->Bob` carries
    /// `since = 2020`; after patching `since → 2099` and adding a fresh `note`, an empty-delta
    /// read serves both from the segment, the base `since` is gone, the endpoints/counts are
    /// unchanged, and it all survives a reopen.
    #[test]
    fn flush_to_segment_materialises_a_core_edge_patch() {
        let (root, _g) = testgen::write_indexed_people("flush_seg_patch_edge_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        // Base edge Alice-KNOWS->Bob carries since=2020; the existing-edge MERGE resolves it
        // and routes the SET to `patch_core_edge` (in-place patch, no duplicate born edge).
        write(
            &graphs,
            "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) \
             SET r.since = 2099, r.note = 'hi'",
        );
        let set_uuid = graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("an edge patch flushes a non-empty delta");

        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            1,
            "one upper segment"
        );
        assert!(
            graphs.writer("people").unwrap().snapshot().is_empty(),
            "delta retired empty"
        );

        // The overlaid prop is served from the segment; the fresh prop too; the base value gone.
        let since = q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) RETURN r.since",
        );
        assert!(
            matches!(since.rows[0][0], Val::Int(2099)),
            "patched edge prop served from the segment: {:?}",
            since.rows[0][0]
        );
        let note = q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) RETURN r.note",
        );
        assert!(
            matches!(&note.rows[0][0], Val::Str(s) if s == "hi"),
            "fresh edge prop served from the segment: {:?}",
            note.rows[0][0]
        );
        // Topology + counts unchanged: both endpoints remain, the edge still traverses, and the
        // node/edge marginals are untouched by a patch.
        let bob = q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name",
        );
        assert!(
            matches!(&bob.rows[0][0], Val::Str(s) if s == "Bob"),
            "the patched edge still traverses to Bob: {:?}",
            bob.rows[0][0]
        );
        assert!(
            matches!(
                q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(3)
            ),
            "an edge patch changes no node count"
        );
        assert!(
            matches!(
                q(&graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0],
                Val::Int(1)
            ),
            "an edge patch changes no edge count"
        );

        // Reopen from disk: the replace row reloads and still serves the patched value.
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the set"
        );
        let since = q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) RETURN r.since",
        );
        assert!(
            matches!(since.rows[0][0], Val::Int(2099)),
            "the patched edge prop reloaded from the segment: {:?}",
            since.rows[0][0]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.1: **T3 segment compaction**. Two flushes stack two upper segments;
    /// `compact_graph_segments` folds them into one merged segment that reads **identically** to
    /// the run it replaces — a births-only pair, a base-node indexed patch (index-removal carry),
    /// and a cross-segment node-row override (newest-wins) all resolve the same before and after,
    /// the stack shrinks to one segment, the id space is preserved, the delta is rebound, and the
    /// merged data survives a reopen.
    #[test]
    fn compact_segments_folds_a_run_into_one() {
        // `write_basic`: Alice/Bob/Carol :Person (name+age indexed, ages 30/25/40),
        // Acme/Globex :Company, base edge Alice-KNOWS->Bob among others.
        let (root, _g, _u) = testgen::write_basic("compact_seg_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {q}"),
            }
        };
        let q = |graphs: &Graphs, query: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let ast = parser::parse(query).unwrap();
            let r = Engine::new(&view, &cache).run(&ast).unwrap();
            r
        };

        // Flush 1: a born node (Dave, indexed name+age), a born edge (Dave-KNOWS->Alice), and a
        // base-node **indexed** patch (Carol's age 40→99 — a below-run index removal + entry).
        write(&graphs, "MATCH (n:Person {name:'Carol'}) SET n.age = 99");
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(
            &graphs,
            "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Alice'})",
        );
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("flush 1 is non-empty");

        // Flush 2: another born node (Frank) and a **cross-segment override** of the same base
        // node's indexed age (Carol 99→77). Carol is a base node, so the write path re-resolves
        // her by key in both flushes; the merge must newest-wins her row (77) and suppress both
        // the base value (40) and segment 1's intermediate value (99) in the index. (Note that a
        // just-flushed *born* key like Dave cannot be re-patched by the write path until Phase 6
        // makes resolve segment-aware — see the plan's 4.1 note (e) — so the override targets the
        // base node Carol, which resolves in both flushes.)
        write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
        write(&graphs, "MATCH (n:Person {name:'Carol'}) SET n.age = 77");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("flush 2 is non-empty");

        let pre = graphs.get("people").unwrap();
        assert_eq!(
            pre.stack().segments().len(),
            2,
            "two upper segments stacked"
        );
        let old_node_total = pre.stack().extents().nodes.total();
        let old_edge_total = pre.stack().extents().edges.total();
        drop(pre);

        // The battery of probes that must read identically before and after the compaction.
        // `Val` has no `PartialEq`, so each probe result is captured as its debug string.
        let probe = |graphs: &Graphs| -> Vec<String> {
            let s = |v: &Val| format!("{v:?}");
            // A one-row scalar, or a marker for the row count when the seek should be empty.
            let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
            // The reverse-adjacency probe onto the base node Alice (its row count matters too).
            let rev = q(
                graphs,
                "MATCH (b:Person {name:'Alice'})<-[:KNOWS]-(a) RETURN a.name",
            );
            vec![
                // 1. cross-segment override of a base node: Carol seeks by her newest age (77);
                //    the base value (40) and segment 1's intermediate value (99) are suppressed.
                scalar(&q(graphs, "MATCH (n:Person {age:77}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:99}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:40}) RETURN n.name")),
                s(&q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age").rows[0][0]),
                // 2. born nodes, each by its born age index.
                scalar(&q(graphs, "MATCH (n:Person {age:50}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
                // 3. node count over summed marginals (3 base Person + Dave + Frank).
                s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
                // 4. born edge traverses to its base target (forward) …
                s(&q(
                    graphs,
                    "MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name",
                )
                .rows[0][0]),
                // 5. … and reverse (incoming KNOWS onto Alice — only the born edge).
                format!("rev_rows={}", rev.rows.len()),
                scalar(&rev),
            ]
        };
        let before = probe(&graphs);
        // Sanity-check the ground truth so a bug can't make before==after both wrong.
        assert_eq!(
            before[0], "Str(\"Carol\")",
            "Carol by newest indexed age 77"
        );
        assert_eq!(before[1], "∅", "segment-1 intermediate age 99 suppressed");
        assert_eq!(before[2], "∅", "base age 40 suppressed");
        assert_eq!(before[3], "Int(77)", "Carol newest age");
        assert_eq!(before[4], "Str(\"Dave\")", "Dave by born age 50");
        assert_eq!(before[5], "Str(\"Frank\")", "Frank by born age 70");
        assert_eq!(before[6], "Int(5)", "3 base Person + 2 born");
        assert_eq!(before[7], "Str(\"Alice\")", "forward edge target");
        assert_eq!(before[8], "rev_rows=1", "one incoming KNOWS on Alice");
        assert_eq!(before[9], "Str(\"Dave\")", "reverse edge source");

        // Compact the run [0, 2) into one segment.
        let set_uuid = graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();

        let post = graphs.get("people").unwrap();
        assert_eq!(post.uuid(), set_uuid, "served the compacted set");
        assert_eq!(post.base_uuid(), base_uuid, "base preserved by compaction");
        assert_eq!(
            post.stack().segments().len(),
            1,
            "run folded into one segment"
        );
        assert_eq!(
            post.stack().extents().nodes.total(),
            old_node_total,
            "node id space invariant under compaction"
        );
        assert_eq!(
            post.stack().extents().edges.total(),
            old_edge_total,
            "edge id space invariant under compaction"
        );
        drop(post);

        // The writer is rebound to the new set (ids unchanged, delta preserved).
        assert_eq!(
            graphs.writer("people").unwrap().core_uuid(),
            set_uuid,
            "delta rebound to the compacted set"
        );

        // Every probe reads identically through the merged segment.
        assert_eq!(probe(&graphs), before, "compaction preserves every read");

        // Reopen from disk: the compacted set + merged segment reload and survive. Re-enable
        // the writable layer so the shared probe closure has a writer (the delta was retired by
        // the flushes and rebound by the compaction, so it reloads empty).
        drop(graphs);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the compacted set"
        );
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            1,
            "merged segment reloaded"
        );
        assert_eq!(
            probe(&graphs),
            before,
            "reopened compaction preserves every read"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.3 (admission policy): the size-tiered auto entry point
    /// [`Graphs::compact_graph_segments_auto`] admits a compaction only when the stack exceeds
    /// `max_upper_segments`, and then folds the selected run through the same T3 writer. Three
    /// similarly-sized flushes stack three segments; `auto` with a threshold ≥ 3 (or 0) is a
    /// no-op, while a threshold of 2 admits and — the three being one tier — folds the whole run
    /// into one. Every read is identical across the no-ops, the fold, and a reopen.
    #[test]
    fn auto_compaction_admits_only_when_over_budget() {
        let (root, _g, _u) = testgen::write_basic("compact_auto_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a node write: {q}"),
            }
        };
        let q = |graphs: &Graphs, query: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let w = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(query).unwrap())
                .unwrap();
            r
        };

        // Three flushes, one born indexed node each ⇒ three similarly-sized upper segments.
        for (name, age) in [("Dave", 50), ("Frank", 60), ("Gina", 70)] {
            write(
                &graphs,
                &format!("MERGE (n:Person {{name:'{name}'}}) SET n.age = {age}"),
            );
            graphs
                .flush_graph_to_segment("people", &vc, &root)
                .unwrap()
                .expect("flush is non-empty");
        }
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            3,
            "three upper segments stacked"
        );

        let probe = |graphs: &Graphs| -> Vec<String> {
            let s = |r: QueryResult| format!("{:?}", r.rows[0][0]);
            vec![
                s(q(graphs, "MATCH (n:Person {age:50}) RETURN n.name")),
                s(q(graphs, "MATCH (n:Person {age:60}) RETURN n.name")),
                s(q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
                s(q(graphs, "MATCH (n:Person) RETURN count(*)")),
            ]
        };
        let before = probe(&graphs);
        assert_eq!(before[0], "Str(\"Dave\")");
        assert_eq!(before[3], "Int(6)", "3 base Person + 3 born");

        // Within budget (threshold ≥ segment count) and disabled (0) are both no-ops.
        assert_eq!(
            graphs
                .compact_graph_segments_auto("people", &vc, &root, 3)
                .unwrap(),
            None,
            "3 segments, threshold 3 ⇒ within budget"
        );
        assert_eq!(
            graphs
                .compact_graph_segments_auto("people", &vc, &root, 0)
                .unwrap(),
            None,
            "threshold 0 ⇒ admission disabled"
        );
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            3,
            "no-op auto calls left the stack untouched"
        );

        // Over budget (threshold 2 < 3): admit and fold. The three are one tier ⇒ whole run.
        let set_uuid = graphs
            .compact_graph_segments_auto("people", &vc, &root, 2)
            .unwrap()
            .expect("threshold 2 admits a compaction");
        let post = graphs.get("people").unwrap();
        assert_eq!(post.uuid(), set_uuid, "served the compacted set");
        assert_eq!(
            post.stack().segments().len(),
            1,
            "the one-tier run folded into a single segment"
        );
        drop(post);
        assert_eq!(
            graphs.writer("people").unwrap().core_uuid(),
            set_uuid,
            "delta rebound to the compacted set"
        );
        assert_eq!(
            probe(&graphs),
            before,
            "auto-compaction preserves every read"
        );

        // Now within budget again (1 segment) ⇒ auto is a no-op.
        assert_eq!(
            graphs
                .compact_graph_segments_auto("people", &vc, &root, 2)
                .unwrap(),
            None,
            "1 segment, threshold 2 ⇒ nothing left to admit"
        );

        // Reopen: the compacted set reloads and every read survives.
        drop(graphs);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            1,
            "merged segment reloaded"
        );
        assert_eq!(
            probe(&graphs),
            before,
            "reopened auto-compaction preserves every read"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.2 (merge hardening): a **base-node delete** materialised in the older
    /// segment of a run folds correctly through a T3 merge. Bob is a base node, so his
    /// tombstone and the `removed` fragments for his two incident base edges are **below-run**
    /// — the merge must *carry* them (nothing beneath the run holds Bob), keeping him and his
    /// edges gone, while the summed marginals net the delete. A born node in the newer segment
    /// tiles above. Every read is identical before and after the compaction and after a reopen.
    #[test]
    fn compact_folds_a_base_delete_across_the_run() {
        // `write_basic`: Alice/Bob/Carol :Person; KNOWS edges Alice→Bob, Bob→Carol, Alice→Carol.
        let (root, _g, _u) = testgen::write_basic("compact_del_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        // Flush 1 (seg 0): DETACH DELETE Bob — a base-node tombstone + `removed` fragments for
        // his incident KNOWS edges (Alice→Bob on Alice's out side, Bob→Carol on Carol's in side).
        write(&graphs, "MATCH (n:Person {name:'Bob'}) DETACH DELETE n");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("delete flush is non-empty");
        // Flush 2 (seg 1): a born node so the run has two segments to fold.
        write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("born flush is non-empty");

        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            2,
            "two upper segments stacked"
        );
        let old_node_total = graphs
            .get("people")
            .unwrap()
            .stack()
            .extents()
            .nodes
            .total();
        let old_edge_total = graphs
            .get("people")
            .unwrap()
            .stack()
            .extents()
            .edges
            .total();

        let probe = |graphs: &Graphs| -> Vec<String> {
            let s = |v: &Val| format!("{v:?}");
            let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
            let fwd = q(
                graphs,
                "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name",
            );
            vec![
                scalar(&q(graphs, "MATCH (n:Person {name:'Bob'}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:25}) RETURN n.name")),
                s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
                s(&q(graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0]),
                // Bob's delete removed Alice→Bob and Bob→Carol; Alice→Carol survives.
                format!("fwd_rows={}", fwd.rows.len()),
                scalar(&fwd),
                scalar(&q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
            ]
        };
        let before = probe(&graphs);
        assert_eq!(before[0], "∅", "Bob gone from the name index");
        assert_eq!(before[1], "∅", "Bob gone from the age index");
        assert_eq!(before[2], "Int(3)", "Alice, Carol, Frank survive");
        assert_eq!(before[3], "Int(1)", "only Alice→Carol KNOWS remains");
        assert_eq!(before[4], "fwd_rows=1", "Alice keeps one KNOWS out-edge");
        assert_eq!(before[5], "Str(\"Carol\")", "…to Carol");
        assert_eq!(before[6], "Str(\"Frank\")", "born Frank by age 70");

        let set_uuid = graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();

        let post = graphs.get("people").unwrap();
        assert_eq!(post.uuid(), set_uuid, "served the compacted set");
        assert_eq!(post.base_uuid(), base_uuid, "base preserved");
        assert_eq!(post.stack().segments().len(), 1, "run folded into one");
        assert_eq!(
            post.stack().extents().nodes.total(),
            old_node_total,
            "node id space invariant"
        );
        assert_eq!(
            post.stack().extents().edges.total(),
            old_edge_total,
            "edge id space invariant"
        );
        drop(post);
        assert_eq!(
            graphs.writer("people").unwrap().core_uuid(),
            set_uuid,
            "delta rebound to the compacted set"
        );
        assert_eq!(
            probe(&graphs),
            before,
            "the carried tombstone + edge removals read identically"
        );

        // Reopen: the below-run tombstone and edge removals reload from the merged segment.
        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the compacted set"
        );
        assert_eq!(probe(&graphs), before, "reopen preserves every read");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.2 (merge hardening): compacting a **partial run** `[1, 3)` with a
    /// segment below it (seg 0) and above it (seg 3) preserves cross-segment precedence. Carol
    /// is patched in every segment (10→20→30→40); the merge folds seg 1⊕seg 2 to their own
    /// newest (30) yet the whole stack still resolves to seg 3's 40 (above the run wins), and
    /// seg 0's below-run value (10) stays superseded — the merged segment's carried index
    /// removal keeps suppressing it. Each flush also births a distinct node so the run's bands
    /// are non-trivial. Reads are identical before/after the compaction and after a reopen.
    #[test]
    fn compact_a_partial_run_preserves_precedence() {
        let (root, _g, _u) = testgen::write_basic("compact_partial_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        // Four flushes: each patches base Carol's indexed age and births one distinct node, so
        // seg k carries Carol=(11·(k+1)) and a born node aged 91+k. Carol's ladder avoids the
        // base ages (Alice 30 / Bob 25 / Carol 40) so a seek pins exactly one node.
        for (k, (dave, dage, cage)) in [
            ("D1", 91, 11),
            ("D2", 92, 22),
            ("D3", 93, 33),
            ("D4", 94, 44),
        ]
        .into_iter()
        .enumerate()
        {
            write(
                &graphs,
                &format!("MERGE (n:Person {{name:'{dave}'}}) SET n.age = {dage}"),
            );
            write(
                &graphs,
                &format!("MATCH (n:Person {{name:'Carol'}}) SET n.age = {cage}"),
            );
            graphs
                .flush_graph_to_segment("people", &vc, &root)
                .unwrap()
                .unwrap_or_else(|| panic!("flush {k} is non-empty"));
        }
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            4,
            "four upper segments stacked"
        );
        let old_node_total = graphs
            .get("people")
            .unwrap()
            .stack()
            .extents()
            .nodes
            .total();
        let old_edge_total = graphs
            .get("people")
            .unwrap()
            .stack()
            .extents()
            .edges
            .total();

        let probe = |graphs: &Graphs| -> Vec<String> {
            let s = |v: &Val| format!("{v:?}");
            let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
            vec![
                // Carol resolves to seg 3's value (above the run), not the merged run's newest.
                s(&q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age").rows[0][0]),
                scalar(&q(graphs, "MATCH (n:Person {age:44}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:33}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:22}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:11}) RETURN n.name")),
                // Every born node — below (D1), within (D2,D3), above (D4) the run — survives.
                scalar(&q(graphs, "MATCH (n:Person {age:91}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:92}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:93}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:94}) RETURN n.name")),
                s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
            ]
        };
        let before = probe(&graphs);
        assert_eq!(before[0], "Int(44)", "Carol = seg 3 (above run)");
        assert_eq!(before[1], "Str(\"Carol\")", "seek age 44 → Carol");
        assert_eq!(before[2], "∅", "merged run's internal 33 superseded");
        assert_eq!(before[3], "∅", "run's 22 superseded");
        assert_eq!(before[4], "∅", "seg 0's below-run 11 superseded");
        assert_eq!(before[5], "Str(\"D1\")", "below-run born node");
        assert_eq!(before[6], "Str(\"D2\")", "within-run born node");
        assert_eq!(before[7], "Str(\"D3\")", "within-run born node");
        assert_eq!(before[8], "Str(\"D4\")", "above-run born node");
        assert_eq!(before[9], "Int(7)", "3 base + 4 born Person");

        // Compact only the middle run [1, 3): seg 0 stays below, seg 3 stays above.
        let set_uuid = graphs
            .compact_graph_segments("people", &vc, &root, 1, 3)
            .unwrap();

        let post = graphs.get("people").unwrap();
        assert_eq!(post.uuid(), set_uuid, "served the compacted set");
        assert_eq!(post.base_uuid(), base_uuid, "base preserved");
        assert_eq!(
            post.stack().segments().len(),
            3,
            "4 segments − 2 merged + 1 = 3"
        );
        assert_eq!(
            post.stack().extents().nodes.total(),
            old_node_total,
            "node id space invariant"
        );
        assert_eq!(
            post.stack().extents().edges.total(),
            old_edge_total,
            "edge id space invariant"
        );
        drop(post);
        assert_eq!(
            probe(&graphs),
            before,
            "partial-run compaction preserves precedence"
        );

        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            3,
            "spliced set reloaded"
        );
        assert_eq!(probe(&graphs), before, "reopen preserves every read");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.2 (merge hardening): a merge whose run **includes a zero-width band** —
    /// seg 0 is a patch-only flush (Carol's age, no born node ⇒ an empty node/edge band) — folds
    /// correctly with a births-carrying seg 1. The contiguity check accepts the zero-width tile,
    /// the patched base row and its carried index removal survive, and the born node reads back.
    #[test]
    fn compact_folds_a_zero_width_band() {
        let (root, _g, _u) = testgen::write_basic("compact_zerowidth_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let q = |graphs: &Graphs, qy: &str| -> QueryResult {
            let gen = graphs.get("people").unwrap();
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(&parser::parse(qy).unwrap())
                .unwrap();
            r
        };

        // Flush 1 (seg 0): patch-only — a base-node index move, no births ⇒ zero-width bands.
        write(&graphs, "MATCH (n:Person {name:'Carol'}) SET n.age = 99");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("patch-only flush is non-empty");
        let gen0 = graphs.get("people").unwrap();
        let seg0 = &gen0.stack().segments()[0];
        assert_eq!(
            seg0.manifest.node_band.0, seg0.manifest.node_band.1,
            "seg 0 has a zero-width node band (patch-only)"
        );
        drop(gen0);
        // Flush 2 (seg 1): a born node so the run mixes a zero-width and a non-empty band.
        write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("born flush is non-empty");

        assert_eq!(
            graphs.get("people").unwrap().stack().segments().len(),
            2,
            "two upper segments"
        );
        let old_node_total = graphs
            .get("people")
            .unwrap()
            .stack()
            .extents()
            .nodes
            .total();

        let probe = |graphs: &Graphs| -> Vec<String> {
            let s = |v: &Val| format!("{v:?}");
            let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
            vec![
                s(&q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age").rows[0][0]),
                scalar(&q(graphs, "MATCH (n:Person {age:99}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:40}) RETURN n.name")),
                scalar(&q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
                s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
            ]
        };
        let before = probe(&graphs);
        assert_eq!(before[0], "Int(99)", "Carol's patched age");
        assert_eq!(before[1], "Str(\"Carol\")", "seek age 99 → Carol");
        assert_eq!(
            before[2], "∅",
            "base age 40 superseded via the carried removal"
        );
        assert_eq!(before[3], "Str(\"Frank\")", "born Frank by age 70");
        assert_eq!(before[4], "Int(4)", "Alice, Bob, Carol, Frank");

        let set_uuid = graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();
        let post = graphs.get("people").unwrap();
        assert_eq!(post.uuid(), set_uuid, "served the compacted set");
        assert_eq!(post.stack().segments().len(), 1, "run folded into one");
        assert_eq!(
            post.stack().extents().nodes.total(),
            old_node_total,
            "node id space invariant"
        );
        drop(post);
        assert_eq!(probe(&graphs), before, "zero-width fold reads identically");

        drop(graphs);
        let graphs = Graphs::open_all(&root, None).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the compacted set"
        );
        assert_eq!(probe(&graphs), before, "reopen preserves every read");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.2 (merge hardening): a merge over an **encrypted** stack writes a fresh
    /// per-segment cipher + KDF header and seals the manifest MAC — mirroring the flush path —
    /// so the merged segment is ciphertext, decrypts on read, and reopens only WITH the key.
    #[test]
    fn compact_encrypts_the_merged_segment() {
        let key: &[u8] = b"an-at-rest-master-key-32byteslong";
        let (root, _g) = testgen::write_indexed_people_keyed("compact_keyed_e2e", Some(key));
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);

        let mut graphs = Graphs::open_all(&root, Some(key)).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a write: {qy}"),
            }
        };
        let age_of = |graphs: &Graphs, name: &str| -> Option<i64> {
            let gen = graphs.get("people").unwrap();
            let snap = graphs
                .writer("people")
                .map(|w| w.delta_snapshot())
                .unwrap_or_else(DeltaSnapshot::empty);
            let view = MergedView::new(gen.as_ref(), snap);
            let r = Engine::new(&view, &cache)
                .run(
                    &parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age"))
                        .unwrap(),
                )
                .unwrap();
            r.rows.first().and_then(|row| match &row[0] {
                Val::Int(n) => Some(*n),
                _ => None,
            })
        };

        // Two flushes stack two encrypted segments (a born node each + a base patch).
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 91");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 92");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();

        assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 2);
        let set_uuid = graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();

        // The merged segment carries its own encryption header + sealed MAC — proof it is
        // ciphertext, not plaintext beside the encrypted core.
        let gen1 = graphs.get("people").unwrap();
        assert_eq!(gen1.stack().segments().len(), 1, "run folded into one");
        let seg = &gen1.stack().segments()[0];
        let header = seg
            .manifest
            .encryption
            .as_ref()
            .expect("merged segment manifest carries an encryption header");
        assert_eq!(header.aead, graph_format::crypto::AEAD_NAME);
        assert!(seg.manifest.mac.is_some(), "merged segment is MAC-sealed");
        drop(gen1);

        // Reads decrypt through the merged segment: Dave/Frank born, Alice newest-wins (92).
        assert_eq!(age_of(&graphs, "Dave"), Some(50), "born Dave decrypts");
        assert_eq!(age_of(&graphs, "Frank"), Some(70), "born Frank decrypts");
        assert_eq!(age_of(&graphs, "Alice"), Some(92), "Alice newest-wins (92)");

        // Reopen WITH the key: the merged encrypted segment reloads and serves.
        drop(graphs);
        let graphs = Graphs::open_all(&root, Some(key)).unwrap();
        assert_eq!(
            graphs.get("people").unwrap().uuid(),
            set_uuid,
            "reopen names the compacted set"
        );
        assert_eq!(
            age_of(&graphs, "Dave"),
            Some(50),
            "Dave decrypts after reopen"
        );
        assert_eq!(
            age_of(&graphs, "Alice"),
            Some(92),
            "Alice = 92 after reopen"
        );
        drop(graphs);

        // Reopen WITHOUT the key is refused (the MAC-sealed encrypted set cannot open).
        assert!(
            Graphs::open_all(&root, None).is_err(),
            "an encrypted compacted set refuses to open without the key"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 5 slice 5.2 (merge hardening): a merge against a **non-filesystem** store uploads
    /// the merged segment, spliced set manifest and `current` pointer through the `ObjectStore`
    /// abstraction (the run's old segments stay in the store for a later GC). A fresh open that
    /// reads *only* through the in-memory store serves the folded data store-natively.
    #[test]
    fn compact_uploads_to_an_object_store() {
        use graph_format::store::mem::MemObjectStore;
        use graph_format::store::ObjectStore as _;

        let (root, _g) = testgen::write_indexed_people("compact_seg_memstore");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mem = Arc::new(MemObjectStore::new());
        load_dir_into_mem(&mem, &root, &root);

        let mut graphs =
            Graphs::open_all_with_store(mem.clone() as Arc<dyn ObjectStore>, None, true, None)
                .unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let base_uuid = graphs.get("people").unwrap().uuid();

        let write = |graphs: &Graphs, qy: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            match parser::parse_statement(qy).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => panic!("expected a node write: {qy}"),
            }
        };
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap();
        assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 2);

        let set_uuid = graphs
            .compact_graph_segments("people", &vc, &root, 0, 2)
            .unwrap();

        // The store now names the compacted set; its manifest references exactly one segment.
        assert_eq!(
            String::from_utf8(mem.read_all("people/current").unwrap())
                .unwrap()
                .trim(),
            set_uuid.0.to_string(),
            "remote current names the compacted set"
        );
        let uploaded_set =
            graph_format::setmanifest::SetManifest::read_via(mem.as_ref(), "people", set_uuid)
                .unwrap();
        assert_eq!(
            uploaded_set.segments.len(),
            1,
            "the uploaded set references the single merged segment"
        );
        // The merged segment's SEGMENT.json is in the store; the two pre-merge dirs also remain
        // (GC is a later phase), so the store holds three segment dirs.
        let seg_uuids = mem.list("people/segments").unwrap();
        assert_eq!(
            seg_uuids.len(),
            3,
            "merged + two pre-merge segment dirs (old ones GC'd later)"
        );
        assert!(
            mem.exists(&format!(
                "people/segments/{}/SEGMENT.json",
                uploaded_set.segments[0].uuid.0
            ))
            .unwrap(),
            "the merged SEGMENT.json was uploaded"
        );

        // Reopen reading ONLY through the mem store: the folded data is served store-natively.
        drop(graphs);
        let graphs =
            Graphs::open_all_with_store(mem.clone() as Arc<dyn ObjectStore>, None, true, None)
                .unwrap();
        let gen = graphs.get("people").unwrap();
        assert_eq!(gen.uuid(), set_uuid, "store reopen names the compacted set");
        assert_eq!(gen.base_uuid(), base_uuid, "base preserved");
        assert_eq!(gen.stack().segments().len(), 1, "merged segment from store");
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
        let names: Vec<String> = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) WHERE n.age >= 50 RETURN n.name").unwrap())
            .unwrap()
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("name not str: {v:?}"),
            })
            .collect();
        assert!(
            names.contains(&"Dave".to_string()) && names.contains(&"Frank".to_string()),
            "both born nodes served from the merged store-native segment: {names:?}"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Number of `*.l0` segment files under `<wal>/<graph>/l0/`.
    fn l0_count(wal_dir: &Path) -> usize {
        let l0 = wal_dir.join("l0");
        std::fs::read_dir(&l0)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("l0"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Phase 4c-B end-to-end through the query overlay: a MERGE-born node and a core
    /// property patch, once flushed to an L0 level, still read back through the full
    /// `MergedView` (label scan **and** index seek), a re-MERGE of the flushed born node
    /// reuses its synthetic id (no duplicate), and everything survives a reopen (the L0
    /// file reloads, the WAL-tail re-MERGE re-resolves against it).
    #[test]
    fn flush_to_l0_overlay_reads_and_born_reuse_survive_reopen() {
        let (root, _g) = testgen::write_indexed_people("flush_overlay_e2e");
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);

        // A query over the writer's full published delta (active memtable ⊕ L0 levels).
        let names_ages = |graphs: &Graphs, q: &str| -> Vec<(String, Option<i64>)> {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let mut out: Vec<(String, Option<i64>)> = Engine::new(&view, &cache)
                .run(&ast)
                .unwrap()
                .rows
                .iter()
                .map(|r| {
                    let name = match &r[0] {
                        Val::Str(s) => s.clone(),
                        v => panic!("name not str: {v:?}"),
                    };
                    let age = match r.get(1) {
                        Some(Val::Int(n)) => Some(*n),
                        _ => None,
                    };
                    (name, age)
                })
                .collect();
            out.sort();
            out
        };
        let write = |graphs: &Graphs, q: &str| {
            let gen = graphs.get("people").unwrap();
            let writer = graphs.writer("people").unwrap();
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a node write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        // MERGE-create Dave (born) and patch a core node (Alice.age = 99).
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");

        // Flush the memtable to an L0 level — the active memtable is now empty.
        let writer = graphs.writer("people").unwrap();
        assert!(writer.flush_to_l0().unwrap());
        assert_eq!(writer.l0_len(), 1);
        assert!(
            writer.snapshot().is_empty(),
            "active memtable freed by flush"
        );
        assert_eq!(l0_count(&writer.wal_dir()), 1, "one L0 file on disk");

        // Read back through the L0 level: index seek finds Dave, label scan lists him,
        // Alice's patched age is served.
        assert_eq!(
            names_ages(
                &graphs,
                "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"
            ),
            vec![("Dave".to_string(), Some(50))],
            "index seek finds the flushed born node"
        );
        assert_eq!(
            names_ages(
                &graphs,
                "MATCH (n:Person {name:'Alice'}) RETURN n.name, n.age"
            ),
            vec![("Alice".to_string(), Some(99))],
            "the flushed core patch is served"
        );
        let all = names_ages(&graphs, "MATCH (n:Person) RETURN n.name");
        assert!(
            all.iter().any(|(n, _)| n == "Dave"),
            "label scan lists the flushed born node: {all:?}"
        );

        // Re-MERGE the flushed born Dave (post-flush, into the active memtable). It must
        // reuse the L0 synthetic id — no duplicate — and the newer age wins.
        write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 55");
        assert_eq!(
            writer.delta_snapshot().born_count(),
            1,
            "re-MERGE reuses the flushed born id, no duplicate"
        );
        assert_eq!(
            names_ages(
                &graphs,
                "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"
            ),
            vec![("Dave".to_string(), Some(55))],
            "the re-MERGE patch (active memtable) wins over the flushed value"
        );

        // Reopen: the L0 file reloads and the WAL-tail re-MERGE re-resolves against it.
        drop(writer);
        drop(graphs);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        assert_eq!(
            graphs.writer("people").unwrap().l0_len(),
            1,
            "reopen reloads L0"
        );
        assert_eq!(
            graphs
                .writer("people")
                .unwrap()
                .delta_snapshot()
                .born_count(),
            1,
            "reopen does not duplicate the born node"
        );
        assert_eq!(
            names_ages(
                &graphs,
                "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"
            ),
            vec![("Dave".to_string(), Some(55))],
            "Dave (age 55) survives the reopen via the L0 file + WAL tail"
        );
        assert_eq!(
            names_ages(
                &graphs,
                "MATCH (n:Person {name:'Alice'}) RETURN n.name, n.age"
            ),
            vec![("Alice".to_string(), Some(99))],
            "Alice's flushed patch survives the reopen"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 4c-B: consolidation folds a flushed L0 level. A born node lives in an L0
    /// segment (not the active memtable); the consolidation dump must still carry it
    /// (proving `frozen.l0` reached the merged view), and `retire` deletes the L0 file
    /// and clears the level stack.
    #[test]
    fn consolidation_folds_a_flushed_l0_level() {
        let (root, _graph) = testgen::write_indexed_people("consolidate_l0");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen0 = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let wal_dir = writer.wal_dir();

        // MERGE-born Dave + a core patch, then flush both into an L0 level.
        for q in [
            "MERGE (n:Person {name:'Dave'}) SET n.age = 50",
            "MATCH (n:Person {name:'Alice'}) SET n.age = 99",
        ] {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
            execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
        }
        assert!(writer.flush_to_l0().unwrap());
        assert_eq!(writer.l0_len(), 1);
        assert!(writer.snapshot().is_empty(), "everything flushed to L0");
        assert_eq!(l0_count(&wal_dir), 1);

        // The injected builder proves the dump folded the L0 level (Dave's MERGE + the
        // merged Alice age), then publishes a canned consolidated generation.
        let new_uuid = uuid::Uuid::from_u128(0x4c0b_0000_0000_0000_0000_0000_0000_0001);
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let build = |dump: &Path, g: &str, dd: &Path| -> Result<()> {
            let nodes = dump_nodes(dump);
            assert!(
                nodes.contains_key("Dave"),
                "the flushed born node must be in the dump: {:?}",
                nodes.keys().collect::<Vec<_>>()
            );
            assert_eq!(
                dump_age(dump, "Alice"),
                Some(99),
                "the flushed core patch must be in the dump"
            );
            assert_eq!(g, "people");
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            Ok(())
        };
        let published = graphs
            .consolidate_graph("people", &cache, &vc, &root, build)
            .unwrap();
        assert_eq!(published.0, new_uuid);

        // Retire folded + deleted the L0 level: no level stack, no L0 file.
        let writer = graphs.writer("people").unwrap();
        assert_eq!(writer.l0_len(), 0, "L0 stack cleared by retire");
        assert_eq!(l0_count(&wal_dir), 0, "L0 file deleted by retire");
        assert!(!root.join("people").join(".consolidate.dump").exists());

        std::fs::remove_dir_all(&root).ok();
    }

    /// A consolidation whose rebuild fails (modelled as the builder erroring before
    /// it publishes anything — the crash window between freeze and the `current`
    /// swap) is non-destructive: the old core keeps serving, the delta stays live,
    /// and the durable write replays on a fresh reopen (the freeze sealed but did not
    /// delete its segments).
    #[test]
    fn failed_consolidation_preserves_the_write_and_old_core() {
        let (root, _graph) = testgen::write_indexed_people("consolidate_crash");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        let gen0 = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let wal_dir = writer.wal_dir();
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
        execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();

        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let build =
            |_d: &Path, _g: &str, _dd: &Path| -> Result<()> { bail!("simulated builder crash") };
        let err = graphs
            .consolidate_graph("people", &cache, &vc, &root, build)
            .unwrap_err();
        assert!(format!("{err:#}").contains("simulated builder crash"));

        // Old core still served (unchanged uuid); delta still live (age 99 overlaid);
        // the scratch dump is cleaned up.
        let gen_after = graphs.get("people").unwrap();
        assert_eq!(gen_after.uuid(), gen0.uuid(), "old core keeps serving");
        assert!(
            !writer.snapshot().is_empty(),
            "delta not retired on failure"
        );
        assert_eq!(
            writer.snapshot().node_patch(0).unwrap().patches.get("age"),
            Some(&Value::Int(99))
        );
        assert!(!root.join("people").join(".consolidate.dump").exists());

        // Durability: a fresh writer over the WAL replays the write.
        let reopened = DeltaWriter::open(
            &wal_dir,
            "people",
            gen0.uuid(),
            gen0.node_count(),
            gen0.edge_count(),
            |op| resolve_op(&gen0, op),
        )
        .unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .node_patch(0)
                .unwrap()
                .patches
                .get("age"),
            Some(&Value::Int(99)),
            "the write survives a failed consolidation + reopen"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// True end-to-end consolidation through the real `slater-build` binary. Ignored
    /// by default — `cargo test -p slater` does not build the builder. Run it with
    /// the binary located via `SLATER_BUILD_BIN` (or on `PATH`):
    /// ```text
    /// cargo build -p slater-build
    /// SLATER_BUILD_BIN=$CARGO_TARGET_DIR/debug/slater-build \
    ///   cargo test -p slater -- --ignored consolidate_via_real_builder
    /// ```
    #[test]
    #[ignore = "spawns the real slater-build binary; see the doc comment"]
    fn consolidate_via_real_builder() {
        let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
        let (root, _graph) = testgen::write_indexed_people("consolidate_real");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let gen0 = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
        execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();

        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        // A post-freeze write (Bob's age → 77) applied while the real builder runs must
        // be carried forward onto the new core by retire (Phase 4a).
        let writer_mid = writer.clone();
        let gen_mid = gen0.clone();
        let new = graphs
            .consolidate_graph("people", &cache, &vc, &root, |d, g, dd| {
                let bob =
                    match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
                        .unwrap()
                    {
                        parser::ast::Statement::Write(w) => w,
                        _ => unreachable!(),
                    };
                execute_write(&writer_mid, gen_mid.as_ref(), &bob, &HashMap::new()).unwrap();
                run_builder(&bin, d, g, dd)
            })
            .unwrap();
        assert_ne!(new.0, gen0.uuid().0, "rebuilt a new generation");

        let gen1 = graphs.get("people").unwrap();
        let read_age = |name: &str| -> Val {
            let view = MergedView::new(
                gen1.as_ref(),
                DeltaSnapshot::from_memtable(writer.snapshot()),
            );
            let ast =
                parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
            let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
            age
        };
        assert!(
            matches!(read_age("Alice"), Val::Int(99)),
            "the real builder folded the delta into the core"
        );
        assert!(
            matches!(read_age("Bob"), Val::Int(77)),
            "the post-freeze write survived on the carried-forward delta"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Every operation of the write grammar, so a new one cannot be added without being
    /// listed here. Each must parse to a mutating statement.
    fn every_write_statement() -> Vec<&'static str> {
        vec![
            // ── node writes (the grammar requires a SET or DELETE after the pattern,
            //    so a bare `MERGE (n:L {k:v})` is not a valid statement) ───────────
            "MERGE (n:Person {name:'Dave'}) SET n.age = 1",
            "MATCH (n:Person {name:'Alice'}) SET n.age = 1",
            "MATCH (n:Person {name:'Alice'}) SET n.age = 1, n.city = 'Oslo'",
            "MATCH (n:Person {name:'Alice'}) DELETE n",
            "MATCH (n:Person {name:'Alice'}) DETACH DELETE n",
            // ── batched (write-`UNWIND`) node writes ─────────────────────────────
            "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
            "UNWIND $rows AS r MATCH (n:Person {name: r.name}) SET n.age = r.age",
            "UNWIND $rows AS r MATCH (n:Person {name: r.name}) DELETE n",
            // ── relationship writes ──────────────────────────────────────────────
            "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'})",
            "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 2020",
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
            // ── admin: rewrites the served generation ────────────────────────────
            "CALL slater.consolidate()",
        ]
    }

    fn acl_json(grants: serde_json::Value) -> Acl {
        let json = serde_json::json!({
            "users": { "u": { "passwordArgon2id": hash_password("pw").unwrap(), "grants": grants } }
        });
        Acl::from_json_str(&json.to_string()).unwrap()
    }

    /// **A read grant must not authorise any write.** Before the writable layer landed the
    /// ACL had only `can_read`, so switching on `delta.enabled` would silently have promoted
    /// every reader into a writer. Every operation of the write grammar is checked.
    #[test]
    fn a_read_only_grant_forbids_every_write_operation() {
        let read_only = acl_json(serde_json::json!({ "people": ["read"] }));
        for q in every_write_statement() {
            let stmt = parser::parse_statement(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
            assert!(
                statement_mutates(&stmt),
                "{q} must be classified as a mutating statement"
            );
            let err = authorize_statement(&read_only, "u", "people", &stmt).expect_err(q);
            assert_eq!(err.code, CODE_FORBIDDEN, "{q}");
            assert!(err.message.contains("write access"), "{q}: {}", err.message);
        }
    }

    /// The same statements are authorised once the user also holds `write`.
    #[test]
    fn a_read_write_grant_authorises_every_write_operation() {
        let rw = acl_json(serde_json::json!({ "people": ["read", "write"] }));
        for q in every_write_statement() {
            let stmt = parser::parse_statement(q).unwrap();
            authorize_statement(&rw, "u", "people", &stmt)
                .unwrap_or_else(|e| panic!("read+write must authorise {q}: {}", e.message));
        }
    }

    /// The write grant is **per graph**: holding it on one graph authorises nothing on
    /// another, and reads never need it.
    #[test]
    fn the_write_grant_is_per_graph_and_reads_stay_allowed() {
        let acl = acl_json(serde_json::json!({
            "people": ["read"],
            "scratch": ["read", "write"],
        }));
        let write =
            parser::parse_statement("MERGE (n:Person {name:'Dave'}) SET n.age = 1").unwrap();
        assert!(authorize_statement(&acl, "u", "scratch", &write).is_ok());
        assert!(
            authorize_statement(&acl, "u", "people", &write).is_err(),
            "a write grant on `scratch` must not leak to `people`"
        );

        let read = parser::parse_statement("MATCH (n:Person) RETURN count(*)").unwrap();
        assert!(!statement_mutates(&read));
        assert!(authorize_statement(&acl, "u", "people", &read).is_ok());
        assert!(authorize_statement(&acl, "u", "scratch", &read).is_ok());
    }

    /// `count(*)` over a **merged** view must net the delta's born rows in and its
    /// suppressed rows out — and must do so without scanning the core (the fast path
    /// reads `live_node_count`). Checked against the executor's own materialising scan,
    /// which is the definition of what a read sees.
    #[tokio::test]
    async fn merged_count_star_nets_born_and_suppressed_rows() {
        let (_root, ctx) =
            build_writable_ctx_caps("merged_count", "slater-build", 1 << 20, 0, 0, 0, 0, 8, 0);
        let writer = ctx.graphs.writer("people").unwrap();
        let gen = ctx.graphs.get("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let try_write = |q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new())
        };
        let write = |q: &str| try_write(q).unwrap();
        let count = |q: &str| -> i64 {
            let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            match rows[0][0] {
                Val::Int(n) => n,
                ref other => panic!("expected an int count, got {other:?}"),
            }
        };
        // The materialising scan — the ground truth the fast path must agree with.
        let scanned = || -> i64 {
            let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
            let ast = parser::parse("MATCH (n) RETURN n.name").unwrap();
            let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            rows.len() as i64
        };
        let check = |expected: i64| {
            assert_eq!(
                count("MATCH (n) RETURN count(*)"),
                expected,
                "whole-graph count"
            );
            assert_eq!(
                count("MATCH (n:Person) RETURN count(*)"),
                expected,
                "labelled count"
            );
            assert_eq!(scanned(), expected, "the scan agrees with the fast path");
        };

        check(3); // Alice, Bob, Carol.
        write("MERGE (n:Person {name:'Dave'}) SET n.age = 1"); // born
        check(4);
        write("MATCH (n:Person {name:'Alice'}) DETACH DELETE n"); // suppress a core row (Alice has edges)
        check(3);
        write("MATCH (n:Person {name:'Dave'}) DELETE n"); // suppress a born row (no edges)
        check(2);
        // A delete of a key that exists nowhere is refused outright, so it can never
        // enter the delta as an inert tombstone and wrongly decrement the count.
        assert!(try_write("MATCH (n:Person {name:'Ghost'}) DELETE n").is_err());
        check(2);
        write("MERGE (n:Person {name:'Alice'}) SET n.age = 31"); // resurrect the core row
        check(3);
    }

    /// The whole-graph metadata shapes — `labels(n)[0]`, `type(r)` and the bare edge
    /// `count(*)` — must stay metadata reads over a delta and agree with the materialising
    /// scan. Deleting a node also kills its incident edges, so the edge count drops by
    /// that node's degree. Fixture: 3 `:Person`, one `Alice-[:KNOWS]->Bob`.
    #[tokio::test]
    async fn merged_metadata_and_edge_counts_track_the_delta() {
        let (_root, ctx) =
            build_writable_ctx_caps("merged_meta", "slater-build", 1 << 20, 0, 0, 0, 0, 8, 0);
        let writer = ctx.graphs.writer("people").unwrap();
        let gen = ctx.graphs.get("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let write = |q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };
        let rows = |q: &str| -> Vec<Vec<Val>> {
            let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
            let ast = parser::parse(q).unwrap();
            let out = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            out
        };
        let one_int = |q: &str| -> i64 {
            let r = rows(q);
            match r[0][0] {
                Val::Int(n) => n,
                ref other => panic!("expected an int, got {other:?}"),
            }
        };
        // The count column of the first group row (`Val` has no `PartialEq`).
        let group_count = |q: &str| -> i64 {
            let r = rows(q);
            match r[0][1] {
                Val::Int(n) => n,
                ref other => panic!("expected an int count, got {other:?}"),
            }
        };
        // The materialising scan — ground truth for the edge count.
        let scanned_edges = || -> i64 { rows("MATCH ()-[r]->() RETURN r").len() as i64 };

        // Baseline: 3 nodes, 1 edge. The bare edge count used to have no fast path at all.
        assert_eq!(one_int("MATCH ()-[r]->() RETURN count(*)"), 1);
        assert_eq!(scanned_edges(), 1);
        assert_eq!(group_count("MATCH (n) RETURN labels(n)[0], count(*)"), 3);
        assert_eq!(group_count("MATCH ()-[r]->() RETURN type(r), count(*)"), 1);

        // A born node adds a label group but no edges.
        write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
        assert_eq!(
            group_count("MATCH (n) RETURN labels(n)[0], count(*)"),
            4,
            "born node counted in the label group"
        );
        assert_eq!(
            one_int("MATCH ()-[r]->() RETURN count(*)"),
            1,
            "born node adds no edges"
        );
        assert_eq!(scanned_edges(), 1);

        // DETACH-deleting a core endpoint also removes the edge incident to it (a plain
        // DELETE would be rejected while the edge is still there).
        write("MATCH (n:Person {name:'Bob'}) DETACH DELETE n");
        assert_eq!(
            one_int("MATCH ()-[r]->() RETURN count(*)"),
            0,
            "Alice→Bob dies with its endpoint"
        );
        assert_eq!(scanned_edges(), 0, "the scan agrees");
        assert_eq!(
            group_count("MATCH (n) RETURN labels(n)[0], count(*)"),
            3,
            "label group drops the deleted node"
        );
        assert!(
            rows("MATCH ()-[r]->() RETURN type(r), count(*)").is_empty(),
            "an empty reltype group is not emitted"
        );
    }

    /// An edge tombstone cannot be netted out of a counter (a deleted **core** edge carries
    /// no edge id), so the edge fast paths must **decline** rather than report a wrong
    /// number — the matcher then produces the right answer.
    #[tokio::test]
    async fn edge_tombstone_makes_the_edge_fast_path_decline_not_lie() {
        let (_root, ctx) = build_writable_ctx_caps(
            "merged_edge_tomb",
            "slater-build",
            1 << 20,
            0,
            0,
            0,
            0,
            8,
            0,
        );
        let writer = ctx.graphs.writer("people").unwrap();
        let gen = ctx.graphs.get("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        assert!(
            MergedView::new(gen.as_ref(), writer.delta_snapshot())
                .live_edge_count()
                .unwrap()
                .is_some(),
            "an empty delta is exactly countable"
        );

        let stmt = match parser::parse_statement(
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
        )
        .unwrap()
        {
            parser::ast::Statement::WriteEdge(w) => w,
            other => panic!("expected an edge delete, got {other:?}"),
        };
        execute_edge_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        assert!(
            view.live_edge_count().unwrap().is_none(),
            "an edge tombstone makes the counter-derived count inexact ⇒ decline"
        );
        // The query still answers correctly, via full execution.
        let ast = parser::parse("MATCH ()-[r]->() RETURN count(*)").unwrap();
        let counted = match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
            Val::Int(n) => n,
            ref other => panic!("expected an int, got {other:?}"),
        };
        assert_eq!(counted, 0, "the deleted edge is suppressed by the matcher");
    }

    /// A delta-born node is a real, readable node, so a plain `MATCH … SET` must be able
    /// to update it — both while it is still in the active memtable and after it has been
    /// flushed to an L0 segment. (It used to resolve the key against the core only, so
    /// updating a node you had just created failed with "use MERGE to create it".)
    #[tokio::test]
    async fn match_set_updates_a_delta_born_node() {
        let (_root, ctx) =
            build_writable_ctx_caps("set_born", "slater-build", 1 << 20, 0, 0, 0, 0, 8, 0);
        let writer = ctx.graphs.writer("people").unwrap();
        let gen = ctx.graphs.get("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let try_write = |q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new())
        };
        let age_of = |name: &str| -> Option<i64> {
            let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
            let ast =
                parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
            let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            rows.first().map(|r| match r[0] {
                Val::Int(n) => n,
                ref other => panic!("expected an int age, got {other:?}"),
            })
        };

        // Born, still in the active memtable → SET must find it.
        try_write("MERGE (n:Person {name:'Dave'}) SET n.age = 1").unwrap();
        try_write("MATCH (n:Person {name:'Dave'}) SET n.age = 2").unwrap();
        assert_eq!(
            age_of("Dave"),
            Some(2),
            "SET on a born node in the memtable"
        );

        // Flush it to an L0 segment, then SET again → must resolve across the levels.
        assert!(writer.flush_to_l0().unwrap(), "born row flushed to L0");
        try_write("MATCH (n:Person {name:'Dave'}) SET n.age = 3").unwrap();
        assert_eq!(age_of("Dave"), Some(3), "SET on a born node flushed to L0");

        // A key that exists in neither the core nor the delta is still a clear error.
        let e = try_write("MATCH (n:Person {name:'Nobody'}) SET n.age = 1").unwrap_err();
        assert!(e.message.contains("node to update"), "got: {}", e.message);
    }

    /// The same invariants once the delta is spread across **sealed L0 levels**: a born
    /// row, its tombstone, and its resurrection each land in a different level, so the
    /// count summary must fold newest-wins across levels rather than sum them.
    #[tokio::test]
    async fn merged_count_star_folds_across_l0_levels() {
        // memtable_bytes = 1 ⇒ every write flushes; trigger 0 ⇒ no compaction, so the
        // levels stay distinct and the cross-level fold is what is under test.
        let (_root, ctx) =
            build_writable_ctx_caps("merged_count_l0", "slater-build", 1, 0, 0, 0, 0, 8, 0);
        let writer = ctx.graphs.writer("people").unwrap();
        let gen = ctx.graphs.get("people").unwrap();
        let cache = BlockCache::new(1 << 20);

        let write = |q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };
        let count = || -> i64 {
            let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
            let ast = parser::parse("MATCH (n:Person) RETURN count(*)").unwrap();
            let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
            match rows[0][0] {
                Val::Int(n) => n,
                ref other => panic!("expected an int count, got {other:?}"),
            }
        };

        write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(count(), 4, "born in L0");

        write("MATCH (n:Person {name:'Dave'}) DELETE n");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(
            count(),
            3,
            "tombstoned in a newer level than it was born in"
        );

        write("MERGE (n:Person {name:'Dave'}) SET n.age = 2");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(
            count(),
            4,
            "a newer MERGE resurrects it: the older tombstone must not still subtract"
        );
        assert!(writer.l0_len() >= 2, "the levels really are distinct");
    }

    /// Phase 4d-ii-a: the write path auto-maintains the delta. With a 1-byte memtable
    /// cap every write flushes to an L0 segment; with a 3-segment compaction trigger the
    /// third flush collapses the stack. Drives `execute_write` + `maybe_maintain_delta`
    /// exactly as the RUN handler does, and confirms the born rows survive.
    #[tokio::test]
    async fn write_path_auto_flushes_and_compacts() {
        let (root, ctx) =
            build_writable_ctx_caps("auto_maint", "slater-build", 1, 3, 0, 0, 0, 8, 0);
        let writer = ctx.graphs.writer("people").unwrap();
        let gen = ctx.graphs.get("people").unwrap();

        let write = |q: &str| {
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a node write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };

        write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(writer.l0_len(), 1, "first write flushed");
        assert!(writer.snapshot().is_empty(), "memtable freed by the flush");

        write("MERGE (n:Person {name:'Erin'}) SET n.age = 2");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(writer.l0_len(), 2, "second write flushed");

        write("MERGE (n:Person {name:'Fay'}) SET n.age = 3");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(
            writer.l0_len(),
            1,
            "third flush hit the compaction trigger and collapsed the stack"
        );

        // All three born rows still read back through the compacted delta.
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
        let names: HashSet<String> = Engine::new(&view, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        for n in ["Dave", "Erin", "Fay"] {
            assert!(
                names.contains(n),
                "born {n} survives flush+compaction: {names:?}"
            );
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 6 closing slice: the write path auto-fires the two **segment-tier** rungs.
    /// With a 1-byte `segmentFlushBytes` every write folds the whole delta into a core
    /// segment (T2); with a 2-segment `maxUpperSegments` the third flush tips the stack
    /// over budget and the same `maybe_maintain_delta` pass compacts a run (T3). Drives
    /// `execute_write` + `maybe_maintain_delta` exactly as the RUN handler does, confirms
    /// the stack grows then collapses, and that every born row survives — including a
    /// reopen from disk (the segments are durable, the delta empty after each flush).
    #[tokio::test]
    async fn write_path_auto_flushes_and_compacts_segments() {
        // memtable_bytes 1 (L0 rungs also fire, harmlessly — the whole delta flushes
        // anyway), l0 trigger 0, no consolidation; segment_flush_bytes 1, max_upper 2.
        let (root, ctx) = build_writable_ctx_caps("auto_seg", "slater-build", 1, 0, 0, 0, 1, 2, 0);
        let writer = ctx.graphs.writer("people").unwrap();

        let write = |q: &str| {
            let gen = ctx.graphs.get("people").unwrap();
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a node write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };
        let segment_count = || ctx.graphs.get("people").unwrap().stack().segments().len();

        write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(
            segment_count(),
            1,
            "first write flushed the delta into a segment"
        );
        assert_eq!(writer.total_bytes(), 0, "delta retired by the flush");

        write("MERGE (n:Person {name:'Erin'}) SET n.age = 2");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        assert_eq!(segment_count(), 2, "second write appended a second segment");

        write("MERGE (n:Person {name:'Fay'}) SET n.age = 3");
        maybe_maintain_delta(&ctx, "people", &writer).await;
        let after = segment_count();
        assert!(
            after < 3,
            "third flush tipped the stack past maxUpperSegments and T3 folded a run: {after} segments"
        );
        assert!(
            after <= 2,
            "the stack is back within the 2-segment budget after compaction: {after}"
        );

        // Every born row reads back through the compacted segment stack.
        let names_through = |gen: &Generation, w: &Arc<DeltaWriter>| -> HashSet<String> {
            let cache = BlockCache::new(1 << 20);
            let view = MergedView::new(gen, w.delta_snapshot());
            let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
            let out: HashSet<String> = Engine::new(&view, &cache)
                .run(&ast)
                .unwrap()
                .rows
                .iter()
                .filter_map(|r| match &r[0] {
                    Val::Str(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            out
        };
        let served = ctx.graphs.get("people").unwrap();
        let names = names_through(served.as_ref(), &writer);
        for n in ["Dave", "Erin", "Fay"] {
            assert!(
                names.contains(n),
                "born {n} survives the segment fold: {names:?}"
            );
        }

        // Reopen the graph from disk with no writable layer: the born rows live in the
        // durable segments (the delta was empty after the last flush), so a fresh read
        // still serves them.
        let reopened = Graphs::open_all(&root, None).unwrap();
        let cache = BlockCache::new(1 << 20);
        let gen = reopened.get("people").unwrap();
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
        let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
        let reopened_names: HashSet<String> = Engine::new(&view, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        for n in ["Dave", "Erin", "Fay"] {
            assert!(
                reopened_names.contains(n),
                "born {n} is durable across a reopen: {reopened_names:?}"
            );
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// Phase 7 slice 7.3: the write path auto-fires the T4 **GC** sweep after a T3 compaction.
    /// With `segmentGcGraceSecs > 0` the sweep that `maybe_maintain_delta` runs after a
    /// compaction folds a run *marks* the run's now-orphaned segment dirs (a `.gcmark` per dir)
    /// but waits out the grace before deleting — so the marker's presence proves the wiring
    /// fired GC without a fold-then-sleep. An explicit immediate sweep then reclaims them.
    #[tokio::test]
    async fn write_path_auto_gc_marks_orphans_after_compaction() {
        // segment_flush_bytes 1 (flush each write), max_upper 2 (compact when >2), grace 3600
        // (the auto-GC marks the orphans but holds them through the grace).
        let (root, ctx) =
            build_writable_ctx_caps("auto_gc", "slater-build", 1, 0, 0, 0, 1, 2, 3600);
        let writer = ctx.graphs.writer("people").unwrap();
        let write = |q: &str| {
            let gen = ctx.graphs.get("people").unwrap();
            let stmt = match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => w,
                _ => panic!("expected a node write: {q}"),
            };
            execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
        };
        // Count the GC grace markers the sweep stamps under `<graph>/.gc/` (a `seg-<uuid>` per
        // orphaned segment observed within the grace).
        let gcmark_count = |root: &Path| -> usize {
            std::fs::read_dir(root.join("people").join(".gc"))
                .map(|rd| {
                    rd.filter_map(|e| e.ok())
                        .filter(|e| e.file_name().to_string_lossy().starts_with("seg-"))
                        .count()
                })
                .unwrap_or(0)
        };

        // Four flushes tip the stack past maxUpperSegments and drive at least one compaction,
        // whose orphaned run dirs the wiring's GC sweep marks.
        for (i, name) in ["Dave", "Erin", "Fay", "Gina"].iter().enumerate() {
            write(&format!(
                "MERGE (n:Person {{name:'{name}'}}) SET n.age = {i}"
            ));
            maybe_maintain_delta(&ctx, "people", &writer).await;
        }
        assert!(
            ctx.graphs.get("people").unwrap().stack().segments().len() <= 2,
            "the stack stayed within the compaction budget"
        );
        let marked = gcmark_count(&root);
        assert!(
            marked >= 1,
            "the auto-GC sweep marked the compacted run's orphaned dirs: {marked}"
        );

        // An immediate explicit sweep reclaims the marked orphans end-to-end.
        let rep = ctx.graphs.gc_orphan_segments("people", &root, 0).unwrap();
        assert!(
            !rep.deleted_segments.is_empty(),
            "the marked orphans are reclaimed: {rep:?}"
        );
        // Only live segments remain, and every born row still reads back.
        let cache = BlockCache::new(1 << 20);
        let served = ctx.graphs.get("people").unwrap();
        assert_eq!(
            seg_dirs(&root).len(),
            served.stack().segments().len(),
            "no orphan dirs linger after the sweep"
        );
        let view = MergedView::new(served.as_ref(), writer.delta_snapshot());
        let names: HashSet<String> = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) RETURN n.name").unwrap())
            .unwrap()
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        for n in ["Dave", "Erin", "Fay", "Gina"] {
            assert!(names.contains(n), "born {n} survives GC: {names:?}");
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn consolidation_due_is_a_fraction_of_core() {
        // Disabled / degenerate cases.
        assert!(!consolidation_due(1_000, 500, 0), "percent 0 disables");
        assert!(!consolidation_due(0, 5, 25), "empty core never fires");
        assert!(
            !consolidation_due(3, 3, 10),
            "core too small for 10% to mean a whole entity (threshold rounds to 0)"
        );
        // 25% of 4 entities = 1: one changed entity fires.
        assert!(consolidation_due(4, 1, 25));
        assert!(!consolidation_due(4, 0, 25), "no delta yet");
        // 10% of 100M entities = 10M: bounded write amplification on a large core.
        assert!(consolidation_due(100_000_000, 10_000_000, 10));
        assert!(!consolidation_due(100_000_000, 9_999_999, 10));
        // No overflow near u64 max.
        assert!(consolidation_due(u64::MAX, u64::MAX / 2, 25));
    }

    #[test]
    fn window_permits_gates_the_fraction_trigger() {
        use crate::cron_window::CronWindow;
        // No window ⇒ a due consolidation is always permitted.
        assert!(window_permits(&None, (3, 15, 6, 3)));
        assert!(window_permits(&None, (12, 15, 6, 3)));

        // A 01:00–05:59 daily window permits inside and defers outside (hour granularity).
        let w = CronWindow::parse("0 1-5 * * *").unwrap();
        assert!(window_permits(&w, (1, 1, 1, 0)), "01:xx is inside");
        assert!(window_permits(&w, (5, 28, 12, 6)), "05:xx is inside");
        assert!(!window_permits(&w, (0, 15, 6, 3)), "00:xx is outside");
        assert!(!window_permits(&w, (12, 15, 6, 3)), "noon is outside");

        // A weekday-only window also gates on the day of week.
        let wd = CronWindow::parse("* 1-5 * * 1-5").unwrap();
        assert!(window_permits(&wd, (2, 10, 6, 3)), "02:xx Wednesday inside");
        assert!(!window_permits(&wd, (2, 10, 6, 0)), "02:xx Sunday deferred");
    }

    /// Phase 4d-ii-b end-to-end through the write path + real builder: a write that
    /// pushes the delta past `deltaCorePercent` of the core auto-fires a background
    /// consolidation, which folds the write into a fresh generation and retires the
    /// delta — no manual `CALL` needed. Ignored by default (spawns `slater-build`).
    #[tokio::test]
    #[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
    async fn write_path_auto_consolidates_at_core_fraction() {
        use std::time::Duration;
        let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
        // The `people` fixture is 3 nodes + 1 edge = 4 entities; 25% = a threshold of 1,
        // so a single write is due. (Flush/compaction left at defaults; hard cap off.)
        let (root, ctx) = build_writable_ctx_caps("auto_consol", &bin, 64 << 20, 4, 25, 0, 0, 8, 0);
        let writer = ctx.graphs.writer("people").unwrap();
        let gen0 = ctx.graphs.get("people").unwrap();

        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
        execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
        assert!(consolidation_due(4, writer.delta_entity_count() as u64, 25));

        // The write-path hook spawns the background consolidation.
        maybe_maintain_delta(&ctx, "people", &writer).await;

        // Wait for the detached consolidation to publish a fresh generation.
        let mut waited = 0u64;
        while ctx.graphs.get("people").unwrap().uuid() == gen0.uuid() {
            assert!(
                waited < 120_000,
                "auto-consolidation did not complete in time"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            waited += 100;
        }
        let gen1 = ctx.graphs.get("people").unwrap();
        assert_ne!(gen1.uuid(), gen0.uuid(), "a fresh generation was published");

        // Alice's write is now baked into the new core; the delta retired.
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::new(gen1.as_ref(), writer.delta_snapshot());
        let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
        let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
        assert!(
            matches!(age, Val::Int(99)),
            "folded write served from the new core"
        );
        assert!(
            !writer.is_consolidating(),
            "consolidation released its claim"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// A ConnCtx over a writable-layer-enabled `people` graph, with `builder_bin`
    /// pointed at the given binary — the harness for the `CALL slater.consolidate()`
    /// trigger (`execute_consolidate`).
    fn build_writable_ctx(tag: &str, builder_bin: &str) -> (PathBuf, Arc<ConnCtx>) {
        build_writable_ctx_caps(tag, builder_bin, 64 << 20, 4, 0, 0, 0, 8, 0)
    }

    /// [`build_writable_ctx`] with explicit delta caps, so a test can drive the auto
    /// flush/compaction/consolidation thresholds (Phase 4d-ii, Phase 6 segment tiers).
    #[allow(clippy::too_many_arguments)]
    fn build_writable_ctx_caps(
        tag: &str,
        builder_bin: &str,
        memtable_bytes: usize,
        l0_compaction_trigger: usize,
        delta_core_percent: usize,
        delta_hard_bytes: usize,
        segment_flush_bytes: usize,
        max_upper_segments: usize,
        segment_gc_grace_secs: u64,
    ) -> (PathBuf, Arc<ConnCtx>) {
        let (root, _graph) = testgen::write_indexed_people(tag);
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();
        let graphs = Arc::new(graphs);
        // A minimal ACL (unused by consolidation, but ConnCtx requires one).
        let acl_path = root.join("acl.json");
        let json = serde_json::json!({
            "users": { "writer": {
                "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"] }
            }}
        });
        std::fs::write(&acl_path, json.to_string()).unwrap();
        let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
        let ctx = Arc::new(ConnCtx {
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
            data_dir: root.clone(),
            builder_bin: builder_bin.to_string(),
            memtable_bytes,
            l0_compaction_trigger,
            segment_flush_bytes,
            max_upper_segments,
            off_heap_l0: false,
            segment_gc_grace_secs,
            delta_core_percent,
            delta_hard_bytes,
            consolidate_window: None,
        });
        (root, ctx)
    }

    /// Drive a durable `SET` on Alice through the writable layer — a small helper for
    /// the consolidation-trigger tests so there is a live delta to fold.
    fn write_alice_age_99(ctx: &Arc<ConnCtx>) {
        let gen0 = ctx.graphs.get("people").unwrap();
        let writer = ctx.graphs.writer("people").unwrap();
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
        execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
    }

    /// The `CALL slater.consolidate()` trigger reaches consolidation and surfaces a
    /// builder failure as a query `Failure` (not a panic), non-destructively: a missing
    /// builder binary fails the rebuild, the old core keeps serving, and the delta stays
    /// live. Proves the RUN-handler → `execute_consolidate` → `consolidate_graph` wiring
    /// (data dir, builder bin, caches, `spawn_blocking`, error propagation).
    #[tokio::test]
    async fn bolt_consolidate_surfaces_a_builder_failure() {
        let (root, ctx) =
            build_writable_ctx("bolt_consolidate_fail", "/nonexistent/slater-build-xyz");
        write_alice_age_99(&ctx);
        let gen0 = ctx.graphs.get("people").unwrap();

        let err = execute_consolidate(&ctx, "people").await.unwrap_err();
        assert!(
            err.message.contains("consolidation failed"),
            "expected a surfaced builder failure, got: {}",
            err.message
        );
        // Non-destructive: old core still served, the write still overlaid.
        assert_eq!(ctx.graphs.get("people").unwrap().uuid(), gen0.uuid());
        let writer = ctx.graphs.writer("people").unwrap();
        assert!(
            !writer.snapshot().is_empty(),
            "the delta must survive a failed consolidation"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// True end-to-end through the Bolt trigger and the real `slater-build` binary:
    /// `CALL slater.consolidate()` folds the delta into a fresh generation, returns its
    /// id as the `generation` column, and retires the delta. Ignored by default (needs
    /// the builder) — run it exactly like `consolidate_via_real_builder`, with
    /// `SLATER_BUILD_BIN` pointing at the built binary.
    #[tokio::test]
    #[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
    async fn bolt_consolidate_trigger_folds_delta_via_real_builder() {
        let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
        let (root, ctx) = build_writable_ctx("bolt_consolidate_real", &bin);
        write_alice_age_99(&ctx);
        let gen0 = ctx.graphs.get("people").unwrap();

        let (cols, rows) = execute_consolidate(&ctx, "people").await.unwrap();
        assert_eq!(cols, vec!["generation".to_string()]);
        let new_uuid = ctx.graphs.get("people").unwrap().uuid();
        assert_ne!(
            new_uuid,
            gen0.uuid(),
            "consolidation rebuilt a new generation"
        );
        assert!(
            matches!(&rows[0][0], PsValue::String(s) if *s == new_uuid.to_string()),
            "the trigger returns the new generation id"
        );
        let writer = ctx.graphs.writer("people").unwrap();
        assert!(
            writer.snapshot().is_empty(),
            "the delta is retired once folded into the core"
        );
        std::fs::remove_dir_all(&root).ok();
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
            data_dir: root.clone(),
            builder_bin: "slater-build".to_string(),
            memtable_bytes: 64 << 20,
            l0_compaction_trigger: 4,
            segment_flush_bytes: 0,
            max_upper_segments: 8,
            off_heap_l0: false,
            segment_gc_grace_secs: 0,
            delta_core_percent: 0,
            delta_hard_bytes: 0,
            consolidate_window: None,
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
            data_dir: root.clone(),
            builder_bin: "slater-build".to_string(),
            memtable_bytes: 64 << 20,
            l0_compaction_trigger: 4,
            segment_flush_bytes: 0,
            max_upper_segments: 8,
            off_heap_l0: false,
            segment_gc_grace_secs: 0,
            delta_core_percent: 0,
            delta_hard_bytes: 0,
            consolidate_window: None,
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
