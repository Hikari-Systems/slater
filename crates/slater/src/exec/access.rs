// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for low-level record access, budgeting and deadline checks.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    pub(crate) fn node_props(&self, id: u64) -> Result<Vec<(u32, Value)>> {
        // A delta-born node (Phase 2c) has no core props record; its properties are
        // the delta patches plus the business key, folded in by `overlay_node_props`.
        if id >= self.gen.core_generation().node_count() {
            return Ok(Vec::new());
        }
        let rec = self.cache.record(
            self.gen.node_props().inner(),
            self.gen.uuid(),
            FileKind::NodeProps,
            id,
        )?;
        columns::decode_props(&rec)
    }

    pub(crate) fn edge_props(&self, id: u64) -> Result<Vec<(u32, Value)>> {
        let delta = self.gen.delta();
        // A delta-born edge's properties live entirely in the delta overlay. Map each
        // patch name to its property-key id; a name absent from the core symbol table has
        // no id and is dropped from this id-keyed view (still readable by name via
        // `RETURN r.p`).
        if !delta.is_empty() && id >= self.gen.core_generation().edge_count() {
            let mut out = Vec::new();
            for (name, value) in delta.edge_patches(id) {
                if let Some(kid) = self.gen.property_key_id(&name) {
                    out.push((kid, value));
                }
            }
            return Ok(out);
        }
        let rec = self.cache.record(
            self.gen.edge_props().inner(),
            self.gen.uuid(),
            FileKind::EdgeProps,
            id,
        )?;
        let mut props = columns::decode_props(&rec)?;
        // A **core** edge patched in place: fold its delta patches over the core record
        // (replace an existing key, append a new one), mirroring the node patch overlay.
        if !delta.is_empty() {
            for (name, value) in delta.edge_patches(id) {
                if let Some(kid) = self.gen.property_key_id(&name) {
                    match props.iter_mut().find(|(k, _)| *k == kid) {
                        Some(slot) => slot.1 = value,
                        None => props.push((kid, value)),
                    }
                }
            }
        }
        Ok(props)
    }

    pub(crate) fn node_label_ids(&self, id: u64) -> Result<Vec<u32>> {
        node_label_ids_par(self.gen, self.cache, id)
    }

    pub(crate) fn outgoing(&self, id: u64) -> Result<Vec<topology::Adj>> {
        read_adj_overlaid(self.gen, self.cache, id, true)
    }

    pub(crate) fn incoming(&self, id: u64) -> Result<Vec<topology::Adj>> {
        read_adj_overlaid(self.gen, self.cache, id, false)
    }

    /// Read a vector index group `[first_record, first_record + count)` from
    /// `vectors.f32.blk` **through the block cache** (D18) — the brute-force KNN
    /// candidate set. Each record decodes to its dense node id + full-precision
    /// vector; the group is contiguous (D10), so this touches only that index's
    /// blocks and they stay warm for repeat queries. When a fanout pool is
    /// configured and the group is at least [`KNN_PAR_MIN`], the per-record reads
    /// (cache lookup + zstd decode) gather in parallel, preserving record order.
    /// What each level above the base holds for `desc`, resolved per level — see the free fn
    /// [`vector_levels`].
    pub(crate) fn vector_levels(&self, desc: &VectorIndexDesc) -> Result<VectorLevels> {
        vector_levels(self.gen, self.cache, desc)
    }

    pub(crate) fn segment_level(&self, desc: &VectorIndexDesc) -> Result<VectorLevel> {
        segment_level(self.gen, self.cache, desc)
    }

    /// The full-precision embeddings the **sealed base index** holds for `wanted`, keyed by node
    /// id. Ids the base does not index are simply absent.
    ///
    /// The one read that recovers a vector D12 routed *out* of the props record: for a node that
    /// was in the index's scope at build time, this is the only copy in the generation, and no
    /// column read can see it. The consolidation needs it to move a de-labelled node's embedding
    /// back into the column store (HIK-122); nothing on the query path does, because the KNN arms
    /// scan the index rather than probe it by id.
    ///
    /// **Batched on purpose.** Neither arm can seek by node id — the brute-force store is in
    /// build-scan order and the `.pq` layout map is unsorted — so a probe is a scan of the index,
    /// and probing per candidate would make a bulk `REMOVE n:Doc` quadratic (candidates × index)
    /// at consolidation time. One pass per index serves the whole set, and an empty `wanted`
    /// (overwhelmingly the common case) reads nothing at all.
    pub(crate) fn base_index_vectors(
        &self,
        desc: &VectorIndexDesc,
        wanted: &HashSet<u64>,
    ) -> Result<HashMap<u64, Vec<f32>>> {
        let mut out = HashMap::new();
        if wanted.is_empty() {
            return Ok(out);
        }
        match desc.mode {
            // `vectors.f32.blk` holds the group at `[first_record, first_record + count)`, in
            // build-scan order.
            AnnMode::BruteForce => {
                for r in desc.first_record..desc.first_record + desc.count {
                    let e = read_vector(self.gen, self.cache, r)?;
                    if wanted.contains(&e.node_id) {
                        out.insert(e.node_id, e.vector);
                    }
                }
            }
            // The `.pq` side table is the layout→id map (v8: the `.vamana` record is pure
            // geometry), and the `.vamana` record's stored vector is **raw** — the ANN-space
            // transform is a navigation device applied at search time, never at rest — so it
            // is the embedding the user wrote. `node_ids` is already resident, so only the
            // matching records are read.
            AnnMode::Vamana { .. } => {
                let Some(ix) = self.gen.vamana_index(&desc.label, &desc.property) else {
                    return Ok(out);
                };
                for (ord, node_id) in ix.pq.node_ids.iter().enumerate() {
                    if !wanted.contains(node_id) {
                        continue;
                    }
                    let v = ix
                        .reader
                        .node(ord as graph_format::vamana::VamanaIndex)?
                        .vector;
                    out.insert(*node_id, v);
                }
            }
        }
        Ok(out)
    }

    pub(crate) fn delta_level(&self, desc: &VectorIndexDesc) -> Result<VectorLevel> {
        delta_level(self.gen, self.cache, desc)
    }

    /// The RW-index for `desc`, advanced to **this query's delta epoch**, or `None` to
    /// brute-force the delta arm.
    ///
    /// The index is a pure function of the query's *own* pinned snapshot: every id the journal
    /// reports as changed is re-resolved through [`delta_vector_for`] against `self.gen`. That
    /// is what makes it impossible for the index to describe a delta the query is not reading —
    /// and the returned epoch is carried back so the caller can re-check it under the read guard
    /// (another query may advance the index between here and there).
    pub(crate) fn rw_delta_index(
        &self,
        desc: &VectorIndexDesc,
    ) -> Result<Option<(SharedIndex, u64)>> {
        let Some(arm) = &self.rw else {
            return Ok(None);
        };
        // An empty delta has nothing to index, and a read-only estate must pay nothing.
        if self.gen.delta().is_empty() {
            return Ok(None);
        }
        let lookup = arm.indexes.ensure(
            EnsureCtx {
                gen: self.gen.uuid(),
                desc,
                epoch: arm.epoch,
                cfg: &arm.cfg,
                journal: &arm.journal,
            },
            || self.gen.delta().node_dense_ids(),
            |id| delta_vector_for(self.gen, self.cache, id, desc),
        )?;
        Ok(match lookup {
            RwLookup::Ready(ix) => Some((ix, arm.epoch)),
            RwLookup::BruteForce => None,
        })
    }

    pub(crate) fn vector_group(&self, first_record: u64, count: u64) -> Result<Vec<VectorEntry>> {
        let ids: Vec<u64> = (first_record..first_record + count).collect();
        let (gen, cache) = (self.gen, self.cache);
        par_gather(self.fanout_pool.as_deref(), &ids, KNN_PAR_MIN, |&g| {
            read_vector(gen, cache, g)
        })
    }

    /// A node's value for property `key`, or `Null` if absent. (An embedding
    /// routed out to the vector store reads as `Null` here — vector *values* are
    /// served by the M5 KNN/`similarity()` path, not by a column read.)
    pub(crate) fn node_prop(&self, id: u64, key: &str) -> Result<Val> {
        // Decode only the requested key from the cached record, skipping the
        // values of the others (root cause 5): a single-property read no longer
        // allocates a `Vec<(u32, Value)>` nor decodes every other value.
        node_prop_par(self.gen, self.cache, id, key)
    }

    /// The value actually stored at the winning level for `(id, key)`, with **no** D12
    /// suppression — see [`node_prop_raw`]. The vector paths want the embedding itself; every
    /// other read wants [`Self::node_prop`].
    pub(crate) fn node_prop_raw(&self, id: u64, key: &str) -> Result<Val> {
        node_prop_raw(self.gen, self.cache, id, key)
    }

    pub(crate) fn edge_prop(&self, id: u64, key: &str) -> Result<Val> {
        edge_prop_par(self.gen, self.cache, id, key)
    }

    /// Resolve a node's label names and named properties — the material a Bolt
    /// `Node` structure carries. Reads route through the block cache like any other
    /// record access, so encoding a returned node reuses already-resident blocks.
    pub fn node_record(&self, id: u64) -> Result<(Vec<String>, NamedProps)> {
        let labels: Vec<String> = self
            .node_label_ids(id)?
            .into_iter()
            .filter_map(|l| self.gen.label_name(l).map(|s| s.to_string()))
            .collect();
        let mut props = self.core_named_props(id)?;
        self.overlay_node_props(id, &mut props);
        self.suppress_indexed_vectors_named(&labels, &mut props);
        Ok((labels, props))
    }

    /// Strip every *indexed* embedding from a node's name-space property map — the
    /// whole-map twin of [`suppress_indexed_vector`], and for the same reason (D12: a
    /// column read of an indexed embedding yields `Null` from the core, so it must yield
    /// `Null` from the delta and the segments too).
    ///
    /// This is also what keeps a delta-written embedding out of the **column store** at
    /// consolidation: the dumper walks node properties through this fold, so an indexed
    /// vector never reaches `intern_props`. It rides the dump's dedicated vector stream
    /// instead. The T2 flush is unaffected — it builds its rows straight from the
    /// memtable, not through the `ReadView`, so the vector still reaches the segment.
    pub(crate) fn suppress_indexed_vectors_named(&self, labels: &[String], named: &mut NamedProps) {
        if !named.iter().any(|(_, v)| matches!(v, Val::Vector(_))) {
            return;
        }
        let indexes = &self.gen.manifest().vector_indexes;
        named.retain(|(k, v)| {
            !(matches!(v, Val::Vector(_))
                && indexes
                    .iter()
                    .any(|d| &d.property == k && labels.contains(&d.label)))
        });
    }

    /// Node `id`'s **core-stack** properties in name space (below the delta): the winning
    /// segment full row when one carries the id, else the base record mapped to names.
    /// Resolving in name space (rather than through the id-keyed [`Self::node_props`])
    /// preserves a segment property whose key is not in the base symbol table. The caller
    /// folds the delta overlay on top ([`Self::overlay_node_props`]).
    pub(crate) fn core_named_props(&self, id: u64) -> Result<NamedProps> {
        if let Some(row) = self.gen.core_stack().resolve_node_row(id)? {
            if row.tombstoned {
                return Ok(Vec::new());
            }
            return Ok(row
                .props
                .into_iter()
                .map(|(k, v)| (k, Val::from_value(v)))
                .collect());
        }
        Ok(self
            .node_props(id)?
            .into_iter()
            .map(|(kid, v)| (self.key_name(kid), Val::from_value(v)))
            .collect())
    }

    /// Edge `id`'s effective properties in name space: the winning segment full row folded
    /// under the delta's edge patches, else the base record (via [`Self::edge_props`], which
    /// already folds patches). The edge analogue of [`Self::core_named_props`].
    pub(crate) fn core_named_edge_props(&self, id: u64) -> Result<NamedProps> {
        if let Some(row) = self.gen.core_stack().resolve_edge_row(id)? {
            if row.tombstoned {
                return Ok(Vec::new());
            }
            let mut out: NamedProps = row
                .props
                .into_iter()
                .map(|(k, v)| (k, Val::from_value(v)))
                .collect();
            // A delta patch on a segment-carried edge wins last-writer-wins.
            let delta = self.gen.delta();
            if !delta.is_empty() {
                for (name, value) in delta.edge_patches(id) {
                    overlay_named(&mut out, &name, Val::from_value(value));
                }
            }
            return Ok(out);
        }
        Ok(self
            .edge_props(id)?
            .into_iter()
            .map(|(kid, v)| (self.key_name(kid), Val::from_value(v)))
            .collect())
    }

    /// Fold the live delta's property patches for node `id` onto `named` (the core
    /// props already resolved into name-space), last-writer-wins: a patched name
    /// replaces the core value, a new name is appended. The empty-delta fast path
    /// (the overwhelming common case) returns immediately. Phase 1c overlays property
    /// overwrites; Phase 2c also seeds a delta-born node's business-key property.
    pub(crate) fn overlay_node_props(&self, id: u64, named: &mut NamedProps) {
        let delta = self.gen.delta();
        if delta.is_empty() {
            return;
        }
        let nd = delta.node_patch(id);
        let replaced = nd.as_ref().is_some_and(|d| d.replaced);
        let born = id >= self.gen.core_generation().node_count();
        // A `SET n = {map}` replace-all discards every core-derived property.
        if replaced {
            named.clear();
        }
        // Seed the anchor business-key property from the delta identity (it is never
        // stored as a patch) when the core props are not its source of truth: a
        // delta-born node has no core row, and a replaced node just dropped it.
        if born || replaced {
            if let Some((_, kname, kval)) = delta.node_identity_by_dense(id) {
                overlay_named(named, &kname, Val::from_value(kval));
            }
        }
        let Some(nd) = nd else {
            return;
        };
        // Fold out removed properties (a no-op after a replace-all, which already
        // cleared them). The anchor key is never in `removed` (the writer forbids it).
        for name in &nd.removed {
            named.retain(|(k, _)| k.as_str() != name.as_str());
        }
        for (name, value) in &nd.patches {
            overlay_named(named, name, Val::from_value(value.clone()));
        }
    }

    /// The outgoing adjacency of node `id` (dst, reltype, edge id) — the edge-walk
    /// surface the consolidation serialiser ([`crate::consolidate`]) uses to emit
    /// every edge exactly once (from its source). Overlays the edge delta (Phase 3):
    /// a delta-born node's edges are its born out-edges, a tombstoned edge (or an edge
    /// to a tombstoned node) is dropped — so a rebuild carries the writes forward.
    pub fn outgoing_adj(&self, id: u64) -> Result<Vec<topology::Adj>> {
        self.outgoing(id)
    }

    /// The incoming adjacency of node `id` (the mirror of [`Self::outgoing_adj`]) —
    /// the edges whose destination is `id`. Overlay-aware in the same way: a
    /// delta-born in-edge is included, an edge the delta tombstones (or an edge from a
    /// tombstoned node) is dropped. Used by the DELETE-conformance incident-degree
    /// check, which must see relationships in *both* directions.
    pub fn incoming_adj(&self, id: u64) -> Result<Vec<topology::Adj>> {
        self.incoming(id)
    }

    /// Does node `id` have **any** incident relationship (outgoing or incoming) in the
    /// overlaid view? The existence half of [`Self::outgoing_adj`]/[`Self::incoming_adj`]:
    /// it short-circuits on the first surviving edge instead of materialising the whole
    /// adjacency `Vec`, so the DELETE-conformance check on a high-degree hub stops at edge 1
    /// rather than decoding (and allocating) its millions of neighbours. Overlay-exact — it
    /// sees a delta-born edge and drops a delta-tombstoned one, exactly as the collecting
    /// readers, because it shares the one [`for_each_adj_overlaid`] fold.
    pub fn has_incident_edge(&self, id: u64) -> Result<bool> {
        Ok(any_adj_overlaid(self.gen, self.cache, id, true)?
            || any_adj_overlaid(self.gen, self.cache, id, false)?)
    }

    /// The edge id of the first `src -[reltype]-> dst` out-edge in the overlaid view, or
    /// `None`. The existence-resolving analogue of a filtered [`Self::outgoing_adj`] `find`:
    /// it pushes the reltype into the CSR decode and stops at the first matching neighbour,
    /// so it never materialises a hub source's out-adjacency to locate one edge. Which edges
    /// are in scope follows the view — over an empty-delta view it returns the core edge id
    /// only (the `MERGE`-idempotency / core-edge-patch resolver's requirement).
    pub fn find_outgoing_edge(&self, src: u64, reltype: u32, dst: u64) -> Result<Option<u64>> {
        find_outgoing_edge_overlaid(self.gen, self.cache, src, reltype, dst)
    }

    /// Resolve a relationship's type name and named properties — the material a
    /// Bolt `Relationship` structure carries.
    pub fn rel_record(&self, id: u64, reltype: u32) -> Result<(String, NamedProps)> {
        let type_name = self.gen.reltype_name(reltype).unwrap_or("").to_string();
        let props = self.core_named_edge_props(id)?;
        Ok((type_name, props))
    }

    pub(crate) fn key_name(&self, kid: u32) -> String {
        self.gen.property_key_name(kid).unwrap_or("?").to_string()
    }

    /// Raw (undecoded) `node_labels.blk` record for a **core** node, read through the
    /// block cache. The bytes are the canonical [`nodelabels::encode_labels_record`]
    /// layout in the core generation's label ids — the consolidation dump byte-copies
    /// them for untouched nodes, skipping decode + re-encode. Caller guarantees
    /// `id < core_generation().node_count()`.
    pub fn raw_node_labels(&self, id: u64) -> Result<crate::cache::BlockRecord> {
        self.cache.record(
            self.gen.node_labels().inner(),
            self.gen.uuid(),
            FileKind::NodeLabels,
            id,
        )
    }

    /// Raw (undecoded) `node_props.blk` record for a **core** node (see
    /// [`Self::raw_node_labels`]). Caller guarantees `id < core node count`.
    pub fn raw_node_props(&self, id: u64) -> Result<crate::cache::BlockRecord> {
        self.cache.record(
            self.gen.node_props().inner(),
            self.gen.uuid(),
            FileKind::NodeProps,
            id,
        )
    }

    /// Raw (undecoded) `edge_props.blk` record for a **core** edge (see
    /// [`Self::raw_node_labels`]). Caller guarantees `id < core edge count`.
    pub fn raw_edge_props(&self, id: u64) -> Result<crate::cache::BlockRecord> {
        self.cache.record(
            self.gen.edge_props().inner(),
            self.gen.uuid(),
            FileKind::EdgeProps,
            id,
        )
    }

    pub(crate) fn check_deadline(&self) -> Result<()> {
        if let Some(d) = self.deadline {
            if Instant::now() >= d {
                return Err(ExecLimit::Deadline.into());
            }
        }
        Ok(())
    }

    /// Charge `n` elements against the query-wide intermediate budget. Called by
    /// every operation that materialises a collection, so cumulative (not just
    /// peak) allocation is bounded — geometric growth like `reduce(acc + acc)`
    /// trips the budget on an early iteration.
    pub(crate) fn charge(&self, n: u64) -> Result<()> {
        // Per-query budget (config `query.maxIntermediate`; 0 disables).
        if self.max_intermediate != 0 {
            let used = self.budget_used.get().saturating_add(n);
            self.budget_used.set(used);
            if used > self.max_intermediate {
                return Err(ExecLimit::IntermediateBudget(self.max_intermediate).into());
            }
        }
        // Server-wide budget (config `query.maxIntermediateGlobal`; 0 disables) —
        // the aggregate guard a per-query cap cannot provide. Charged even when the
        // per-query budget is off, and refunded in full when the query ends.
        if let Some(g) = self.global_budget {
            if g.limit() != 0 {
                self.global_charged
                    .set(self.global_charged.get().saturating_add(n));
                if !g.try_charge(n) {
                    return Err(ExecLimit::GlobalBudget(g.limit()).into());
                }
            }
        }
        Ok(())
    }

    /// Charge `n` *transient* walk elements against the scan budget (config
    /// `query.maxScan`; 0 disables). Cumulative like [`charge`](Self::charge) so a
    /// geometric blow-up trips early, but — unlike `charge` — it touches neither the
    /// retained per-query budget nor the server-wide aggregate: count-pushdown work
    /// holds no memory, so there is nothing for a concurrent query to compete over.
    pub(crate) fn charge_scan(&self, n: u64) -> Result<()> {
        if self.max_scan != 0 {
            let used = self.scan_used.get().saturating_add(n);
            self.scan_used.set(used);
            if used > self.max_scan {
                bail!(
                    "query exceeded the scan budget of {} elements (query.maxScan)",
                    self.max_scan
                );
            }
        }
        Ok(())
    }

    /// Charge `n` chain-walk elements, routed by retention. In count-pushdown mode
    /// (`count_acc` set) the walk tallies and discards every row, frees each adjacency
    /// buffer per chunk, and holds only a structurally bounded frontier — nothing is
    /// retained, so the charge is transient ([`charge_scan`](Self::charge_scan)). In
    /// row-building mode the same elements materialise, so it is the retained
    /// [`charge`](Self::charge). This is the split that lets a memory-flat `count(*)`
    /// run to the timeout without being gated by the tight memory budget, while a
    /// materialising walk stays bounded exactly as before.
    pub(crate) fn charge_walk(&self, n: u64) -> Result<()> {
        if self.count_acc.get().is_some() {
            self.charge_scan(n)
        } else {
            self.charge(n)
        }
    }

    /// In count-pushdown mode (`count_acc` set), tally one completed row and return
    /// `true` so the caller skips materialising it. `false` in normal row-building
    /// mode. Charging is unchanged and happens at the call site either way, so the
    /// intermediate budget bounds a counted walk exactly as it bounds a materialised
    /// one.
    pub(crate) fn count_tally(&self) -> bool {
        match self.count_acc.get() {
            Some(n) => {
                self.count_acc.set(Some(n + 1));
                true
            }
            None => false,
        }
    }

    /// Refund this query's whole global-budget charge. Idempotent: a second call
    /// (e.g. `Drop` after `run` already released) refunds nothing.
    pub(crate) fn release_global(&self) {
        if let Some(g) = self.global_budget {
            g.release(self.global_charged.replace(0));
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────
}
