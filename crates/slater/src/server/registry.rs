// SPDX-License-Identifier: Apache-2.0
//! `impl Graphs` — the multi-graph registry and generation lifecycle.
//!
//! Split out of `server.rs` as a child module (a pure relocation). The struct,
//! its fields and the shared helpers stay in the parent, reached via `use super::*`.

use super::*;

impl Graphs {
    /// The retained at-rest master key as a byte slice.
    ///
    /// `master_key` is an `Option<Zeroizing<Vec<u8>>>`, so plain `as_deref()` stops
    /// at `&Vec<u8>` (`Zeroizing`'s `Deref` target) rather than reaching `&[u8]` —
    /// this collapses both hops in one place.
    fn master_key_bytes(&self) -> Option<&[u8]> {
        self.master_key.as_deref().map(Vec::as_slice)
    }

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
            master_key: master_key.map(|k| zeroize::Zeroizing::new(k.to_vec())),
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
    pub(crate) fn live_acl_digest(&self) -> Result<Option<String>> {
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
    pub(crate) fn check_manifest_policy(
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
    pub(crate) fn current_generations(&self) -> Vec<Arc<Generation>> {
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
    pub(crate) fn swap_lock(&self, name: &str) -> Result<std::sync::MutexGuard<'_, ()>> {
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
    pub(crate) fn swap_locked(
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
                self.master_key_bytes(),
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
    pub(crate) fn adopt_published_generation(
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
        ) = match self.master_key_bytes() {
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
                master_key: self.master_key_bytes(),
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
        ) = match self.master_key_bytes() {
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
                master_key: self.master_key_bytes(),
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
