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

use crate::acl::AclHandle;
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
use slater_delta::{DeltaSnapshot, OpResolution, WalOp};

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
                let gen = Generation::open_with_store_opts(
                    store.as_ref(),
                    &name,
                    master_key,
                    verify_integrity,
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
            writers: HashMap::new(),
        })
    }

    /// Bring the writable layer online: open a [`DeltaWriter`] per served graph,
    /// replaying each graph's WAL against its current generation. A relative
    /// `cfg.wal_dir` is resolved under `data_dir`; one graph's segments live under
    /// `<wal_dir>/<graph>/`. Called once at boot only when `cfg.enabled`. Idempotent
    /// per graph — a graph that fails to open its writer aborts boot (a durable
    /// write layer that silently isn't there is worse than a hard failure).
    pub fn enable_writable_layer(&mut self, cfg: &DeltaConfig, data_dir: &Path) -> Result<()> {
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
            let writer = DeltaWriter::open(
                &dir,
                name,
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
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

        // Dump the merged (core ⊕ delta) view to a scratch script beside the graph.
        let dump_path = data_dir.join(name).join(".consolidate.cypher");
        let dump_res = (|| -> Result<()> {
            let view = MergedView::new(
                core.as_ref(),
                DeltaSnapshot::with_levels(frozen.snapshot.clone(), frozen.l0.clone()),
            );
            let engine = Engine::new(&view, cache);
            let mut file = std::io::BufWriter::new(
                std::fs::File::create(&dump_path)
                    .with_context(|| format!("create consolidation dump {dump_path:?}"))?,
            );
            crate::consolidate::serialise_merge_dump(&engine, &view, &mut file)?;
            std::io::Write::flush(&mut file).context("flush consolidation dump")?;
            Ok(())
        })();
        if let Err(e) = dump_res {
            let _ = std::fs::remove_file(&dump_path);
            return Err(e).with_context(|| format!("serialise consolidation dump for '{name}'"));
        }

        // Rebuild. A builder failure leaves the delta live (no retire) and the old
        // core serving; propagate the error after removing the scratch dump.
        if let Err(e) = build(&dump_path, name, data_dir) {
            let _ = std::fs::remove_file(&dump_path);
            return Err(e).with_context(|| format!("rebuild consolidated generation for '{name}'"));
        }
        let _ = std::fs::remove_file(&dump_path);

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
}

/// Spawn the configured `slater-build` binary to rebuild `graph` from the `dump`
/// script into `data_dir`, publishing a fresh generation — the production `build`
/// seam for [`Graphs::consolidate_graph`]. A bare `builder_bin` resolves on `PATH`.
/// A non-zero exit is an error, so the caller keeps the old core serving. The
/// invocation mirrors a normal business-key `MERGE` import (no `--pk`).
pub fn run_builder(builder_bin: &str, dump: &Path, graph: &str, data_dir: &Path) -> Result<()> {
    let status = std::process::Command::new(builder_bin)
        .arg("--input")
        .arg(dump)
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
    let mut graphs = Graphs::open_all_with_store(store, master_key.as_deref(), verify_integrity)?;
    graphs.set_manifest_policy(Some(PathBuf::from(&cfg.acl_path)), cfg.require_acl_stamp);
    graphs
        .verify_manifest_policy()
        .context("manifest authentication policy")?;
    if cfg.delta.enabled {
        graphs
            .enable_writable_layer(&cfg.delta, Path::new(cfg.data_dir()))
            .context("enable writable layer")?;
        info!(wal_dir = %cfg.delta.wal_dir, "writable layer enabled");
    }
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
                    match parser::parse_statement(&query)
                        .map_err(|e| Failure::from_query_error(&e))?
                    {
                        parser::ast::Statement::Write(stmt) => {
                            let out = execute_write(w, gen.as_ref(), &stmt, &param_vals)?;
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
    let (src, _reltype, dst) = op.edge_keys().expect("node_key None ⇒ edge op");
    OpResolution::Edge {
        src: one(src),
        dst: one(dst),
    }
}

/// The outcome of probing a write's business key against the current-core range
/// index. Distinguishing *absent* from *ambiguous*/*unindexed* is what lets a
/// `MERGE` create a delta-born node only when the key is genuinely new (Phase 2c).
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
/// equality probe). The overlay's dense-id read index is built from a `Unique` hit.
fn resolve_business_key(gen: &Generation, label: &str, key: &str, value: &Value) -> KeyResolution {
    let labels = [label.to_string()];
    let Some(idx) = crate::plan::index_for(gen, &labels, key) else {
        return KeyResolution::Unindexed;
    };
    let Some(reader) = gen.range_index(&idx) else {
        return KeyResolution::Unindexed;
    };
    let Ok(ids) = reader.lookup_eq(value) else {
        return KeyResolution::Unindexed;
    };
    match ids.as_slice() {
        [] => KeyResolution::Absent,
        [only] => KeyResolution::Unique(*only),
        _ => KeyResolution::Ambiguous,
    }
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

/// Execute one durable write: build the WAL op from the parsed statement +
/// parameters, resolve the anchor's business key to a current-core dense id, and
/// hand it to the writer (WAL append + fsync commit + memtable apply + publish).
/// Returns an empty result — read-back is a separate `MATCH … RETURN` over the
/// overlaid view. A `RETURN` after a write is not yet supported.
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
    let op = match &stmt.op {
        WriteOp::Set(sets) => {
            let mut patches = Vec::with_capacity(sets.len());
            for (prop, expr) in sets {
                patches.push((prop.clone(), write_value(expr, params, "a SET value")?));
            }
            WalOp::UpsertNode {
                label: stmt.label.clone(),
                key: stmt.key.clone(),
                value: key_value,
                patches,
            }
        }
        // DETACH is accepted but a no-op until the Phase 3 topology overlay removes
        // incident edges; the node is tombstoned either way.
        WriteOp::Delete { detach: _ } => WalOp::DeleteNode {
            label: stmt.label.clone(),
            key: stmt.key.clone(),
            value: key_value,
        },
    };
    let is_set = matches!(op, WalOp::UpsertNode { .. });
    let resolved = resolve_node_op(writer, gen, &op, is_set, stmt.upsert)?;
    writer
        .write(op, OpResolution::Node(resolved))
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Resolve a node write's business key to its dense-id context for the WAL op. `Unique`
/// → the core id; a MERGE-create (`is_set && upsert`) on an `Absent` key → a born
/// synthetic id (reusing one already flushed to L0, else `None` to allocate); a DELETE
/// of a born node → its synthetic id resolved across the whole delta; every other
/// absent / ambiguous / unindexed case is a clear error. Shared by the single and
/// batched (write-UNWIND) node write paths so their semantics cannot drift.
fn resolve_node_op(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    op: &WalOp,
    is_set: bool,
    upsert: bool,
) -> std::result::Result<Option<u64>, Failure> {
    let (label, key, value) = op.node_key().expect("resolve_node_op is for node ops only");
    Ok(match resolve_business_key(gen, label, key, value) {
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
        // A `MATCH … SET` (update-only) whose key matches no core node.
        KeyResolution::Absent => {
            return Err(Failure::new(
                CODE_EXECUTION,
                format!(
                    "no {label}({key} = …) node to update: the business key matches no existing \
                     node (use MERGE to create it)"
                ),
            ))
        }
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
    let is_set = matches!(stmt.op, WriteOp::Set(_));
    let mut ops: Vec<(WalOp, OpResolution)> = Vec::with_capacity(rows.len());
    for row in &rows {
        let key_value = eval_row_value(
            &stmt.key_value,
            var,
            row,
            params,
            "the anchor business-key value",
        )?;
        let op = match &stmt.op {
            WriteOp::Set(sets) => {
                let mut patches = Vec::with_capacity(sets.len());
                for (prop, expr) in sets {
                    patches.push((
                        prop.clone(),
                        eval_row_value(expr, var, row, params, "a SET value")?,
                    ));
                }
                WalOp::UpsertNode {
                    label: stmt.label.clone(),
                    key: stmt.key.clone(),
                    value: key_value,
                    patches,
                }
            }
            WriteOp::Delete { detach: _ } => WalOp::DeleteNode {
                label: stmt.label.clone(),
                key: stmt.key.clone(),
                value: key_value,
            },
        };
        let resolved = resolve_node_op(writer, gen, &op, is_set, stmt.upsert)?;
        ops.push((op, OpResolution::Node(resolved)));
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

/// Whether the core already carries an edge `src -[reltype]-> dst` — the `MERGE`
/// idempotency check that stops a re-`MERGE` of an existing core edge from adding a
/// duplicate delta-born edge. Scans only the source's core outgoing adjacency (bounded
/// by its out-degree) over an empty-delta view, so it sees core edges only (a born
/// duplicate is already prevented by the memtable's identity idempotency).
fn core_edge_exists(
    gen: &Generation,
    src: u64,
    reltype: u32,
    dst: u64,
) -> std::result::Result<bool, Failure> {
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
        .any(|a| a.reltype == reltype && a.neighbour.0 == dst))
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

    // A MERGE of an edge whose endpoints are both existing core nodes is idempotent —
    // skip the write if the core already has it (a born duplicate would double the edge
    // on traversal). If either endpoint is delta-born there can be no matching core
    // edge, so no check is needed.
    if stmt.op == EdgeWriteOp::Create {
        if let (Some(s), Some(d)) = (src_core, dst_core) {
            if core_edge_exists(gen, s, reltype_id, d)? {
                // A bare re-MERGE of a core edge stays an idempotent no-op. Patching a
                // *core* edge's properties in place needs a core-edge-id overlay (a
                // distinct mechanism, not wired yet), so reject it clearly rather than
                // silently drop the SET.
                if !patches.is_empty() {
                    return Err(Failure::new(
                        CODE_EXECUTION,
                        format!(
                            "cannot SET properties on the existing base relationship \
                             (:{})-[:{}]->(:{}): editing a core edge's properties is not yet \
                             supported — only delta-born relationships carry editable properties",
                            stmt.src.label, stmt.reltype, stmt.dst.label
                        ),
                    ));
                }
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
    writer
        .write(op, OpResolution::Edge { src, dst })
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
    let new_uuid = tokio::task::spawn_blocking(move || {
        graphs.consolidate_graph(&graph, &cache, &vector_cache, &data_dir, |dump, g, dd| {
            run_builder(&builder_bin, dump, g, dd)
        })
    })
    .await
    .map_err(|e| Failure::new(CODE_EXECUTION, format!("consolidation task failed: {e}")))?
    .map_err(|e| Failure::new(CODE_EXECUTION, format!("consolidation failed: {e:#}")))?;
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
            delta_core_percent: 0,
            delta_hard_bytes: 0,
            consolidate_window: String::new(),
            builder_bin: "slater-build".to_string(),
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
        let stmt =
            match parser::parse_statement("MATCH (n:Person {name:'Alice'}) DELETE n").unwrap() {
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
        write("MATCH (n:Person {name:'Bob'}) DELETE n");

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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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

        // Delete Bob (the edge's destination) through the write path.
        let stmt = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) DELETE n").unwrap()
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
    /// not yet supported and is rejected clearly. (`write_indexed_people` carries a
    /// core edge Alice-KNOWS->Bob with `since = 2020`.)
    #[test]
    fn edge_properties_end_to_end() {
        let (root, _g) = testgen::write_indexed_people("edge_props_3");
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root)
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

        // Patching a CORE edge's properties in place is rejected (deferred mechanism).
        let err = run_write(
            "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 7",
        )
        .unwrap_err();
        assert!(
            err.message
                .contains("editing a core edge's properties is not yet supported"),
            "core-edge property patch rejected: {}",
            err.message
        );
        // A bare re-MERGE of that same core edge is still an idempotent no-op (not an error).
        run_write("MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'})").unwrap();

        // Durable across a reopen: the born edge's patched properties replay.
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
            "edge properties are durable across a reopen"
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            let text = std::fs::read_to_string(dump)?;
            assert!(
                text.contains("MERGE (n:Person {name: 'Alice'}) SET n.age = 99;"),
                "dump should carry the merged age:\n{text}"
            );
            assert!(
                !text.contains("age = 77"),
                "the post-freeze write must not be in the frozen dump:\n{text}"
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
        assert!(!root.join("people").join(".consolidate.cypher").exists());
        assert_eq!(
            wal_count(&wal_dir),
            1,
            "only the post-freeze segment remains"
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
            let text = std::fs::read_to_string(dump)?;
            assert!(
                text.contains("MERGE (n:Person {name: 'Dave'})"),
                "the flushed born node must be in the dump:\n{text}"
            );
            assert!(
                text.contains("SET n.age = 99"),
                "the flushed core patch must be in the dump:\n{text}"
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
        assert!(!root.join("people").join(".consolidate.cypher").exists());

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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
        assert!(!root.join("people").join(".consolidate.cypher").exists());

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
            .enable_writable_layer(&delta_cfg(&wal), &root)
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

    /// Phase 4d-ii-a: the write path auto-maintains the delta. With a 1-byte memtable
    /// cap every write flushes to an L0 segment; with a 3-segment compaction trigger the
    /// third flush collapses the stack. Drives `execute_write` + `maybe_maintain_delta`
    /// exactly as the RUN handler does, and confirms the born rows survive.
    #[tokio::test]
    async fn write_path_auto_flushes_and_compacts() {
        let (root, ctx) = build_writable_ctx_caps("auto_maint", "slater-build", 1, 3, 0, 0);
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
        let (root, ctx) = build_writable_ctx_caps("auto_consol", &bin, 64 << 20, 4, 25, 0);
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
        build_writable_ctx_caps(tag, builder_bin, 64 << 20, 4, 0, 0)
    }

    /// [`build_writable_ctx`] with explicit delta caps, so a test can drive the auto
    /// flush/compaction/consolidation thresholds (Phase 4d-ii).
    fn build_writable_ctx_caps(
        tag: &str,
        builder_bin: &str,
        memtable_bytes: usize,
        l0_compaction_trigger: usize,
        delta_core_percent: usize,
        delta_hard_bytes: usize,
    ) -> (PathBuf, Arc<ConnCtx>) {
        let (root, _graph) = testgen::write_indexed_people(tag);
        let wal = root.join("_wal");
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root)
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
