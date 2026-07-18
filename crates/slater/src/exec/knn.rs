// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for kNN (Vamana) search.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Expand each input row with the `k` nearest neighbours from the named vector
    /// index, binding the `YIELD` outputs (`node`, `score`). The candidate set is
    /// the index group read through the block cache; scoring/selection is the pure
    /// [`vector::brute_force_knn`] over it (D26 — `score` is the distance, ascending).
    pub(crate) fn apply_vector_call(&self, table: Table, vc: &VectorCallClause) -> Result<Table> {
        let (ord, desc) = self
            .gen
            .manifest()
            .vector_indexes
            .iter()
            .enumerate()
            .find(|(_, d)| d.label == vc.label && d.property == vc.property)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no vector index on (:{} {{{}}}) — db.idx.vector.queryNodes needs one",
                    vc.label,
                    vc.property
                )
            })?;
        // Capture the small descriptor bits so the per-row loop does not hold the
        // manifest borrow (it also calls `self` methods to read candidates). `ord`
        // (the index's position) keys its resident matrix in the vector-index pool.
        let ord = ord as u32;
        let metric = desc.metric;
        let dim = desc.dim as usize;
        let first_record = desc.first_record;
        let count = desc.count;
        let mode = desc.mode.clone();
        let desc = desc.clone();

        // The levels above the base carry vectors the sealed base index cannot: a node
        // embedded since the build has no entry in it at all, and a node *re*-embedded
        // since the build has a stale one.
        //
        // Kept as *separate levels* rather than one flattened overlay, because each level has
        // a different set of levels above it and therefore a different suppression set (see
        // `VectorLevels`).
        //
        // The **segments** level is served *per segment* (HIK-113): a segment whose live
        // embedded set crossed the floor at flush/merge carries its own sealed Vamana index and
        // is beam-searched; a smaller (or pre-feature) segment is brute-forced over its own
        // sidecar ids. Each segment suppresses everything *newer* (the delta ∪ every newer
        // segment) in its own scan — `superseded_above` — folded once per query in
        // [`Self::segments_knn`] inside the per-row loop below.
        //
        // The base's suppression set is the sidecar union over every segment: the base sits below
        // all of them, and a base entry any segment supersedes must lose in the base's own scan.
        // A re-embed (`ids`) and a **value** removal always suppress. A **label** removal is
        // suppressed *conditionally* (HIK-118): it means the node left the index's scope, but a
        // later `SET n:Doc` (in the delta or a newer segment) puts it back — and then the base's
        // vector is the live one and must score again. So a `label_removal` id suppresses the base
        // only while the node is **not** currently in scope. `vector_indexed` resolves the
        // effective label set (the same resolve the fold uses), built once per query here — not
        // per row — and reuses the label reads already on the hot path.
        let mut above_base_segments: HashSet<u64> = HashSet::new();
        if !self.gen.core_stack().is_singleton() {
            for seg in self.gen.core_stack().segments() {
                if let Some(v) = &seg.vectors {
                    above_base_segments.extend(v.ids(&desc.label, &desc.property).iter().copied());
                    above_base_segments.extend(
                        v.value_removals(&desc.label, &desc.property)
                            .iter()
                            .copied(),
                    );
                    for &id in v.label_removals(&desc.label, &desc.property) {
                        if !vector_indexed(self.gen, self.cache, id, &desc)? {
                            above_base_segments.insert(id);
                        }
                    }
                }
            }
        }

        // The **delta** level is served by the FreshDiskANN RW-index (`crate::rwindex`) — an
        // in-memory Vamana over the fresh set, advanced to this query's delta epoch and read
        // at exactly that cut. It replaces what used to be an O(delta) `ResidentMatrix`
        // allocate-and-normalise on the hot path of *every single query* — ~300 MB at 768 dim
        // over a 10⁵-vector overlay.
        //
        // Caching a matrix per level instead was never an option: the vector pool charges
        // `matrix_bytes` to its budget and never evicts it, so a per-level matrix would grow
        // the pinned set without bound as segments accumulate (the Σ-over-levels pinning trap,
        // D63). The RW-index is not in that pool at all — it is derived state bounded by the
        // delta, with its own `maxVectors` valve.
        //
        // `RwLookup::BruteForce` (kill switch off, delta below `minVectors` or above
        // `maxVectors`, or an index another query has already advanced *past* our epoch) falls
        // back to exactly the gather-and-scan that shipped before: the same answer, a
        // different cost.
        let rw_ix = self.rw_delta_index(&desc)?;
        let rw = rw_ix
            .as_ref()
            .and_then(|(ix, epoch)| rwindex::read_at_epoch(ix, *epoch));
        // Only when the index is not serving do we pay for the delta walk.
        let brute_delta = match &rw {
            Some(_) => None,
            None => Some(self.delta_level(&desc)?),
        };

        // Everything *newer* than the segments — the delta's suppression set, from whichever
        // arm is serving it. The RW-index maintains it incrementally, so the fast path never
        // re-walks the delta to compute it.
        let owned_above_segments = brute_delta.as_ref().map(|l| l.superseded());
        let above_segments: &HashSet<u64> = match (&owned_above_segments, &rw) {
            (Some(s), _) => s,
            (None, Some(ix)) => ix.superseded(),
            (None, None) => unreachable!("one of the two delta arms is always present"),
        };
        let matrix_of = |entries: Vec<VectorEntry>| -> Result<Option<vector::ResidentMatrix>> {
            if entries.is_empty() {
                return Ok(None);
            }
            Ok(Some(vector::ResidentMatrix::from_entries(
                dim, metric, entries,
            )?))
        };
        let delta_matrix = match brute_delta {
            Some(l) => matrix_of(l.entries)?,
            None => None,
        };

        // A vector index is built over the **base** generation and is immutable, so a
        // node deleted since the build is still in it. The delta/stack tombstones are
        // the only place that delete can take effect on this path — without this the
        // KNN arms would hand back a deleted node as a live `Val::Node` (every other
        // read path already suppresses them; see `suppress_tombstoned_in_place`).
        //
        // A node a *newer* level re-embedded is suppressed for the same reason: this level's
        // vector for it is stale. It must lose in this level's **scan**, not in the merge
        // afterwards — `merge_topk` cannot drop it late without risking the k-th slot (see
        // there). Which levels are "newer" is what differs per arm, and it is the whole point
        // of the split: the base is below both levels, the segments are below only the delta,
        // and nothing at all is above the delta.
        let delta = self.gen.delta();
        let stack = self.gen.core_stack();
        let tombstoned = |id: u64| -> Result<bool> {
            Ok(delta.is_tombstoned(id)
                || (!stack.is_singleton() && stack.is_node_tombstoned(id)?))
        };
        // `above_base` = everything either level above the base supersedes. Tested as two set
        // probes rather than a materialised union, so the delta's half can stay borrowed from
        // the RW-index instead of being cloned per query.
        let live_fn = |id: u64| -> Result<bool> {
            Ok(!above_base_segments.contains(&id)
                && !above_segments.contains(&id)
                && !tombstoned(id)?)
        };
        // Nothing sits above the delta, so its only suppression is the tombstone. Both delta
        // arms already drop tombstoned nodes as they are built (`delta_vector_for` resolves
        // them to `Silent`), so this is defence in depth — but the delta *is* a scanned level
        // like any other, and the RW-index needs a `live` gate anyway to keep a suppressed node
        // a navigable waypoint rather than pruning it from the walk.
        let delta_live_fn = |id: u64| -> Result<bool> { Ok(!tombstoned(id)?) };
        // A pure-core generation with an empty delta can have no tombstones and no overlay,
        // so the read-only estate pays nothing at all for this.
        let live: Option<vector::LivePredicate> = if delta.is_empty() && stack.is_singleton() {
            None
        } else {
            Some(&live_fn)
        };

        // The bound YIELD names introduced into scope, in YIELD order.
        let mut new_vars: Vec<String> = Vec::new();
        for (_, bound) in &vc.yields {
            if !table.cols.contains(bound) && !new_vars.contains(bound) {
                new_vars.push(bound.clone());
            }
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        // The brute-force arm prefers the resident, pre-decoded matrix (decode +
        // normalize once per generation, then scan resident memory — no per-query
        // gather/allocation). It falls back to the up-front gather when no vector
        // pool is wired or the matrix would not fit the vector-index budget. The
        // Vamana arm navigates per query and reads nothing here.
        let matrix = match (&mode, self.vec_cache) {
            (AnnMode::BruteForce, Some(pool)) => {
                let expected = count as usize * dim * std::mem::size_of::<f32>()
                    + count as usize * std::mem::size_of::<u64>();
                pool.matrix_or(self.gen.uuid(), ord, expected, || {
                    vector::ResidentMatrix::from_entries(
                        dim,
                        metric,
                        self.vector_group(first_record, count)?,
                    )
                })?
            }
            _ => None,
        };
        let entries = match (&mode, &matrix) {
            (AnnMode::BruteForce, None) => Some(self.vector_group(first_record, count)?),
            _ => None,
        };

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let k = match self.eval(&vc.k, &scope, None)? {
                Val::Int(n) if n >= 0 => n as usize,
                other => bail!(
                    "db.idx.vector.queryNodes k must be a non-negative integer, got {}",
                    other.to_display()
                ),
            };
            let query = self.eval_query_vector(&vc.query_vec, &scope)?;
            if query.len() != dim {
                bail!(
                    "query vector has dimension {} but the (:{} {{{}}}) index is {}-dimensional",
                    query.len(),
                    vc.label,
                    vc.property,
                    dim
                );
            }
            // Both arms produce the same `score` (the metric distance, ascending) —
            // brute force scans every candidate exactly; Vamana navigates by PQ in
            // resident memory and re-ranks the beam exactly (D32).
            let neighbours = match &mode {
                AnnMode::BruteForce => match &matrix {
                    Some(m) => vector::brute_force_knn_matrix_par(
                        self.fanout_pool.as_deref(),
                        m,
                        &query,
                        k,
                        KNN_PAR_MIN,
                        live,
                    )?,
                    None => vector::brute_force_knn_par(
                        self.fanout_pool.as_deref(),
                        entries.as_ref().unwrap(),
                        &query,
                        k,
                        metric,
                        KNN_PAR_MIN,
                        live,
                    )?,
                },
                AnnMode::Vamana { medoid, nav, .. } => {
                    self.vamana_knn(vc, *medoid, *nav, metric, &query, k, live)?
                }
            };
            // Fold the levels above the base in. Each level has already suppressed — in its own
            // scan/walk — every node a *newer* level supersedes, so the merge is a straight
            // scored fold. See `vector::merge_topk` for why it must not dedup here instead.
            let scan = |m: &Option<vector::ResidentMatrix>,
                        live: vector::LivePredicate|
             -> Result<Vec<vector::Neighbour>> {
                match m {
                    None => Ok(Vec::new()),
                    Some(m) => vector::brute_force_knn_matrix_par(
                        self.fanout_pool.as_deref(),
                        m,
                        &query,
                        k,
                        KNN_PAR_MIN,
                        Some(live),
                    ),
                }
            };
            // The delta arm: the RW-index's beam walk, or the gathered brute force. The
            // **exact** scorer is the same `vector::distance` every other arm re-ranks with
            // (D32/D29), so all three levels' scores are on one scale and `merge_topk`
            // interleaves them correctly rather than silently.
            let fresh = match &rw {
                Some(ix) => ix
                    .graph()
                    .search(
                        &query,
                        k,
                        self.beam_width,
                        |v| vector::distance(metric, &query, v) as f32,
                        delta_live_fn,
                    )?
                    .into_iter()
                    .map(|h| vector::Neighbour {
                        node_id: h.node_id,
                        score: h.exact as f64,
                    })
                    .collect(),
                None => scan(&delta_matrix, &delta_live_fn)?,
            };
            // The **segments** level: one beam per sealed segment, brute force per unsealed one,
            // each suppressed by everything newer than it (`superseded_above`).
            let segs = self.segments_knn(&desc, &query, k, above_segments, &tombstoned)?;
            let neighbours = if segs.is_empty() && fresh.is_empty() {
                neighbours
            } else {
                vector::merge_topk([neighbours, segs, fresh], k)
            };
            // A node id can reach the merged top-k from at most one level: the level that
            // holds its *effective* vector. A duplicate is therefore always a suppression
            // bug — and a silent one, because the stale copy has already taken a slot from a
            // live candidate by the time anyone could notice.
            debug_assert_eq!(
                neighbours
                    .iter()
                    .map(|n| n.node_id)
                    .collect::<HashSet<_>>()
                    .len(),
                neighbours.len(),
                "duplicate node id in the merged top-k — a level failed to suppress: {neighbours:?}"
            );
            for nb in neighbours {
                let mut r = row.clone();
                for bound in &new_vars {
                    let output = vc
                        .yields
                        .iter()
                        .find(|(_, b)| b == bound)
                        .map(|(o, _)| o.as_str())
                        .unwrap_or("");
                    r.push(match output {
                        "node" => Val::Node(nb.node_id),
                        "score" => Val::Float(nb.score),
                        _ => Val::Null,
                    });
                }
                // Apply the optional YIELD ... WHERE over the yielded row.
                if let Some(w) = &vc.where_ {
                    let row_scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &row_scope, None)?) != Some(true) {
                        continue;
                    }
                }
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// The `AnnMode::Vamana` arm: a greedy beam search over the disk-native graph,
    /// navigating by the **resident PQ estimate** (in memory, no IO) and reading
    /// full vectors + adjacency only for the frontier through the vector-index pool
    /// (coalesced by block), re-ranking the beam by the **exact** metric distance so
    /// the returned `score` matches the brute-force contract (D32). The resident set
    /// is PQ codes only — never a full in-memory graph.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn vamana_knn(
        &self,
        vc: &VectorCallClause,
        medoid: u64,
        nav: AnnNav,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
        live: Option<vector::LivePredicate>,
    ) -> Result<Vec<vector::Neighbour>> {
        let (label, property) = (vc.label.as_str(), vc.property.as_str());
        let pool = self.vec_cache.ok_or_else(|| {
            anyhow::anyhow!("vector-index cache is not configured; cannot serve a Vamana index")
        })?;
        let index = self.gen.vamana_index(label, property).ok_or_else(|| {
            anyhow::anyhow!("Vamana index files for (:{label} {{{property}}}) are not open")
        })?;
        // The base's index is keyed in the pool by the generation uuid; `None`-live means every
        // node is live (a pure-core estate with no overlay).
        self.beam_over_index(
            pool,
            self.gen.uuid(),
            index.ord,
            index.reader.inner(),
            &index.pq,
            medoid,
            nav,
            metric,
            query,
            k,
            self.beam_width,
            |id| match live {
                Some(f) => f(id),
                None => Ok(true),
            },
        )
    }

    /// One beam search over a sealed Vamana/PQ index — the base's or a **segment's** (HIK-113).
    /// The only differences between the two callers are which files back it (`reader`/`resident`/
    /// `medoid`), the pool key (`gen_id` — a generation uuid for the base, a **segment uuid** for
    /// a segment; the two spaces cannot collide), and the beam width. Everything the search
    /// itself does — PQ-estimated navigation, one coalesced block read per expansion, the exact
    /// re-rank under the true metric, the `HOLE` + `live` suppression that keeps a dead node a
    /// navigable waypoint, the D26 node-id tie-break — is identical, so it lives here once.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn beam_over_index(
        &self,
        pool: &VectorIndexCache,
        gen_id: graph_format::ids::Generation,
        ord: u32,
        reader: &BlockFileReader,
        resident: &ResidentPq,
        medoid: u64,
        nav: AnnNav,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
        beam_width: usize,
        live: impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<vector::Neighbour>> {
        // The **record** count, holes included — a hole is a legal, navigable neighbour, so
        // this (never the live count) bounds-checks a neighbour ordinal. Using the live count
        // here would reject valid ordinals and silently cut recall.
        let n = resident.len();
        if n == 0 || k == 0 {
            return Ok(Vec::new());
        }
        // The shared (base + sealed-segment) navigator choke: refuse an `InnerProduct` discriminator
        // on a non-Dot index before dispatching. The base index is also checked at generation open
        // (`validate_vamana_index`), but a sealed **segment** carries its own `nav` and has no
        // open-time metric context (`SegmentVamanaSet::open_if_present_via` never sees the metric —
        // it lives in the base descriptor), so a forged `nav: inner_product` on a cosine/L2 segment
        // would otherwise reach `AdcTable::new_ip` here and mis-navigate. Fail closed instead
        // (HIK-137 phase 4).
        nav.check_metric(metric, "vector index navigation")?;
        // PQ navigates in the space the codebook was trained in. HIK-137: an `InnerProduct` index
        // was trained on the RAW vectors and is navigated by the IP-ADC estimate (−⟨q, x̂⟩) with the
        // raw query — NO `ann_query` augmentation. `Augmented` (cosine/L2/legacy-Dot) is unchanged:
        // it maps the query into the L2-reduced ANN space and navigates by squared-L2 ADC.
        let adc = match nav {
            AnnNav::InnerProduct => AdcTable::new_ip(&resident.codebook, query)?,
            AnnNav::Augmented => {
                let qn = graph_format::pq::ann_query(
                    metric,
                    query,
                    resident.codebook.params.dim as usize,
                )?;
                AdcTable::new(&resident.codebook, &qn)?
            }
        };
        let hits = beam_search(
            vamana::BeamParams {
                medoid: medoid as u32,
                beam_width,
                k,
                num_nodes: n,
            },
            |i| adc.estimate(resident.codes_of(i as usize)),
            |i| {
                // One coalesced block read per expansion (cached in the vector pool).
                let rec = pool.record(reader, gen_id, ord, i as u64)?;
                let node = vamana::decode_node(&rec)?;
                Ok((node.vector, node.neighbours))
            },
            |v| vector::distance(metric, query, v) as f32,
            |i| {
                let node_id = resident.node_ids[i as usize];
                if node_id == graph_format::pq::HOLE {
                    return Ok(None);
                }
                if live(node_id)? {
                    Ok(Some(node_id))
                } else {
                    Ok(None)
                }
            },
        )?;
        Ok(hits
            .into_iter()
            .map(|h| vector::Neighbour {
                node_id: h.node_id,
                score: h.exact as f64,
            })
            .collect())
    }

    /// The **segments** level of a KNN read (HIK-113): every core segment, folded
    /// **newest → oldest**, each contributing its live embeddings suppressed by everything above
    /// it — the delta (`above_segments`) plus every *newer* segment. A segment that sealed a
    /// Vamana index is beam-searched; one that did not (below the floor, or pre-feature, or a
    /// deleted/corrupt pair) is brute-forced over its own sidecar ids. `None` sealed index ⇒
    /// brute force is the whole compatibility story.
    ///
    /// The accumulator `acc` is exactly `superseded_above(i)` at segment `i`: it starts at the
    /// delta's suppression set and grows by each visited (newer) segment's `ids ∪ removals`.
    /// Because every older segment suppresses an id a newer one touched, a node reaches the
    /// merged top-k from at most one segment (the newest that still holds it live) — which is
    /// what keeps `merge_topk`'s no-dedup fold correct.
    pub(crate) fn segments_knn(
        &self,
        desc: &VectorIndexDesc,
        query: &[f32],
        k: usize,
        above_segments: &HashSet<u64>,
        tombstoned: &impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<vector::Neighbour>> {
        let stack = self.gen.core_stack();
        if stack.is_singleton() {
            return Ok(Vec::new());
        }
        let (label, property) = (desc.label.as_str(), desc.property.as_str());
        let segs = stack.segments();
        let mut acc: HashSet<u64> = above_segments.clone();
        let mut out: Vec<vector::Neighbour> = Vec::new();
        // Newest → oldest: `segs` is oldest→newest, so iterate in reverse.
        for seg in segs.iter().rev() {
            let Some(sidecar) = &seg.vectors else {
                continue;
            };
            let ids = sidecar.ids(label, property);
            let label_removals = sidecar.label_removals(label, property);
            let value_removals = sidecar.value_removals(label, property);
            if ids.is_empty() && label_removals.is_empty() && value_removals.is_empty() {
                continue;
            }
            // This segment's live gate: suppress everything newer (`acc`), the tombstoned, and
            // any node that no longer effectively carries the index's label. `acc` is read here
            // and only extended *after* the segment is processed, so the two never overlap.
            let live = |id: u64| -> Result<bool> {
                Ok(!acc.contains(&id)
                    && !tombstoned(id)?
                    && vector_indexed(self.gen, self.cache, id, desc)?)
            };
            match seg
                .vector_graph
                .as_ref()
                .and_then(|g| g.get(label, property))
            {
                Some(ix) => out.extend(self.segment_vamana_knn(
                    seg.manifest.segment_uuid,
                    ix,
                    desc.metric,
                    query,
                    k,
                    live,
                )?),
                None => {
                    // Brute force this segment's *own* embeddings (its rows carry the vector).
                    // Apply the live gate while gathering — a brute force has no navigation, so a
                    // suppressed node is simply excluded (no waypoint to preserve), and
                    // pre-filtering keeps the gathered set the exact live set the scan ranks.
                    let mut entries: Vec<VectorEntry> = Vec::new();
                    for &id in ids {
                        if !live(id)? {
                            continue;
                        }
                        if let Some(row) = seg.reader.node_row(id)? {
                            if row.tombstoned {
                                continue;
                            }
                            if let Some((_, Value::Vector(v))) =
                                row.props.iter().find(|(k, _)| k == &desc.property)
                            {
                                entries.push(VectorEntry {
                                    node_id: id,
                                    vector: v.clone(),
                                });
                            }
                        }
                    }
                    if !entries.is_empty() {
                        out.extend(vector::brute_force_knn_par(
                            self.fanout_pool.as_deref(),
                            &entries,
                            query,
                            k,
                            desc.metric,
                            KNN_PAR_MIN,
                            None,
                        )?);
                    }
                }
            }
            // `acc` is only read through `live` above (a shared borrow NLL ends at its last use),
            // so it is free to grow here: fold this segment's touched ids into the suppression
            // set for every older segment. A re-embed (`ids`) and a **value** removal always
            // suppress an older level's entry. A **label** removal is conditional (HIK-118): if
            // the node is back in scope, an *older segment* may still hold its live vector (the
            // re-label did not move it), and that vector must surface — so a re-labelled id does
            // not enter `acc`. When it is still out of scope, the `live` gate's `vector_indexed`
            // check would exclude it anyway; adding it to `acc` keeps the suppression explicit and
            // costs nothing.
            acc.extend(ids.iter().copied());
            acc.extend(value_removals.iter().copied());
            for &id in label_removals {
                if !vector_indexed(self.gen, self.cache, id, desc)? {
                    acc.insert(id);
                }
            }
        }
        Ok(out)
    }

    /// A beam search over one segment's sealed Vamana index, keyed in the vector-index pool by
    /// the **segment uuid** (in the `gen` slot) + the segment-local ordinal. See
    /// [`Self::beam_over_index`].
    pub(crate) fn segment_vamana_knn(
        &self,
        seg_uuid: graph_format::ids::Generation,
        ix: &SegmentVamanaIndex,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
        live: impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<vector::Neighbour>> {
        let pool = self.vec_cache.ok_or_else(|| {
            anyhow::anyhow!("vector-index cache is not configured; cannot serve a segment index")
        })?;
        self.beam_over_index(
            pool,
            seg_uuid,
            ix.ord,
            ix.reader.inner(),
            &ix.pq,
            ix.medoid,
            // HIK-137 phase 3: a Dot segment seals IP-native and carries `nav: InnerProduct` on its
            // `SealedVamanaMeta`; dispatch on it so the segment beam navigates by the IP-ADC estimate,
            // exactly as the base does. A cosine/L2 (or legacy) segment is `Augmented`.
            ix.nav,
            metric,
            query,
            k,
            self.temp_beam_width(),
            live,
        )
    }

    /// Evaluate an expression that must produce a query vector: a `vecf32([...])`
    /// literal, a stored `Vector`, or a list of numbers (a `$param` arrives as a
    /// list). Anything else is a type error.
    pub(crate) fn eval_query_vector(&self, e: &Expr, scope: &Scope) -> Result<Vec<f32>> {
        match self.eval(e, scope, None)? {
            Val::Vector(v) => Ok(v),
            Val::List(xs) => xs
                .iter()
                .enumerate()
                .map(|(i, x)| embed_component(i, x, "query vector"))
                .collect(),
            other => bail!(
                "query vector must be a vecf32([...]) literal or numeric list, got {}",
                other.to_display()
            ),
        }
    }
}
