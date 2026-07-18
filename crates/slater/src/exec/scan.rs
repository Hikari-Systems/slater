// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for candidate scanning and node/rel filtering.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Candidate node ids for a chosen scan strategy, as a **lazy** bounded-window
    /// stream (drain it with [`Engine::next_candidates`]).
    ///
    /// The two full-width sweeps — `AllNodes` and a plain `LabelScan`, the only strategies
    /// whose size is the *graph's*, not the query's — are produced a window at a time and
    /// never materialised. A pushed `LIMIT` therefore stops the scan itself, instead of
    /// merely truncating a row loop over an already-built `Vec` of the whole id space
    /// (733 MB of `Vec<u64>` on the 91.6M-node graph, allocated before the first row and
    /// charged to no budget — the defect this replaces). The scan's resident footprint is
    /// now one window, [`CAND_WINDOW_MAX`] ids, whatever the graph's size.
    ///
    /// Every other strategy is bounded by an index seek or a precomputed posting and is
    /// materialised eagerly, exactly as before, then tombstone-suppressed. Anchor scans are
    /// deliberately charged to neither budget (a point lookup must stay ~free — see
    /// `shortest_path_any_succeeds_under_tiny_budget` — and `maxScan` meters *walk* work —
    /// see `hub_count_one_hop_answered_by_degree_terminal`); what they touch is instead
    /// counted in [`Engine::anchor_ids_scanned`].
    ///
    /// A `LabelScan` is only lazy over a *pure core with no delta*: with a segment stack
    /// or a write delta, membership is a sorted union of base ∪ segment-born ∪ delta-born
    /// ids (a node can gain or lose the label above the base), which cannot be produced in
    /// ascending order window-by-window — so that shape keeps the eager fold.
    pub(crate) fn candidate_stream<'c>(&self, scan: &NodeScan) -> Result<CandidateStream<'c>> {
        let ids = match scan {
            // The dense id space, produced lazily. `node_count` is the *scan bound* (base
            // + every segment's born band + the delta's born ids), and per-window
            // tombstone suppression drops deleted ids exactly as the eager sweep did — so
            // this arm is correct for every stack/delta shape.
            NodeScan::AllNodes => {
                return Ok(CandidateStream::sweep(None, self.gen.node_count()));
            }
            // Pure core, no delta: re-derive the label's ids window-by-window from the
            // node-label column (the same single pass `collect_nodes_with_label` makes,
            // just resumable).
            NodeScan::LabelScan { label_id }
                if self.gen.core_stack().is_singleton() && self.gen.delta().is_empty() =>
            {
                return Ok(CandidateStream::sweep(
                    Some(*label_id),
                    self.gen.node_count(),
                ));
            }
            // Already bounds-checked + deduped by the planner; yield as-is. An
            // empty list is a seek that matched no node.
            NodeScan::IdSeek { ids } => ids.clone(),
            NodeScan::RangeEq { index, key } => {
                let mut ids = self
                    .gen
                    .range_index(index)
                    .expect("planner only picks open indexes")
                    .lookup_eq(key)?;
                // Core stack (below the delta): suppress base hits the segments supersede,
                // union the segments' matching born/patched ids, then restore ascending
                // order for the delta overlay below.
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        stack.fold_index_eq(&mut ids, label, prop, key)?;
                        ids.sort_unstable();
                        ids.dedup();
                    }
                }
                let delta = self.gen.delta();
                if !delta.is_empty() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        // Moved-indexed-value overlay: a core node whose indexed property
                        // was patched is still listed at its *old* value in the ISAM.
                        // Drop hits whose patched value moved out of the seek, and add
                        // core nodes whose patched value moved in (inserted in sorted
                        // position so the ascending order holds).
                        ids.retain(|&x| delta.core_hit_survives_eq(x, prop, key));
                        for id in delta.moved_core_ids_in_index_eq(label, prop, key) {
                            if let Err(pos) = ids.binary_search(&id) {
                                ids.insert(pos, id);
                            }
                        }
                        // Delta-born nodes (Phase 2c) are not in the core ISAM — append
                        // the synthetic ids whose indexed property equals `key`, so a
                        // created node is found by an equality seek (Phase 2d). Born ids
                        // sort after every core id, so the ascending order holds.
                        // Tombstoned ids are dropped by the suppression below.
                        ids.extend(delta.born_ids_in_index_eq(label, prop, key));
                    }
                }
                ids
            }
            NodeScan::RangeRange { index, lo, hi } => {
                let mut ids = self
                    .gen
                    .range_index(index)
                    .expect("planner only picks open indexes")
                    .lookup_range(
                        lo.as_ref().map(|(v, _)| v),
                        lo.as_ref().map(|(_, i)| *i).unwrap_or(true),
                        hi.as_ref().map(|(v, _)| v),
                        hi.as_ref().map(|(_, i)| *i).unwrap_or(true),
                    )?;
                // Core stack index fragments (below the delta), mirroring `RangeEq`.
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        stack.fold_index_range(
                            &mut ids,
                            label,
                            prop,
                            lo.as_ref().map(|(v, _)| v),
                            lo.as_ref().map(|(_, i)| *i).unwrap_or(true),
                            hi.as_ref().map(|(v, _)| v),
                            hi.as_ref().map(|(_, i)| *i).unwrap_or(true),
                        )?;
                        ids.sort_unstable();
                        ids.dedup();
                    }
                }
                // Mirrors the `RangeEq` overlay above: relocate patched core nodes in
                // the range index, then append matching delta-born nodes (Phase 2d).
                let delta = self.gen.delta();
                if !delta.is_empty() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        let lo_v = lo.as_ref().map(|(v, _)| v);
                        let lo_i = lo.as_ref().map(|(_, i)| *i).unwrap_or(true);
                        let hi_v = hi.as_ref().map(|(v, _)| v);
                        let hi_i = hi.as_ref().map(|(_, i)| *i).unwrap_or(true);
                        ids.retain(|&x| {
                            delta.core_hit_survives_range(x, prop, lo_v, lo_i, hi_v, hi_i)
                        });
                        for id in
                            delta.moved_core_ids_in_index_range(label, prop, lo_v, lo_i, hi_v, hi_i)
                        {
                            if let Err(pos) = ids.binary_search(&id) {
                                ids.insert(pos, id);
                            }
                        }
                        ids.extend(
                            delta.born_ids_in_index_range(label, prop, lo_v, lo_i, hi_v, hi_i),
                        );
                    }
                }
                ids
            }
            // LabelScan under a segment stack or a write delta (HIK-104). Membership is a
            // sorted union of the base label column, the stack's re-decided carriers and the
            // delta's born/added-label ids — which the eager path built into one `Vec` (the
            // 733 MB base collect) before the first row. Stream it as a k-way merge instead:
            // the base column is a lazy `LabelCol` source (minus the stack-overridden ids),
            // the write-bounded overlay is one `Mat` source, and per-window `sort`+`dedup`
            // reproduces the eager union exactly, one window resident.
            NodeScan::LabelScan { label_id } => {
                let stack = self.gen.core_stack();
                let delta = self.gen.delta();
                let label = self.gen.label_name(*label_id).map(str::to_string);
                // Stack overlay: `exclude` are the ids whose base membership the stack
                // overrides (removed from the base sweep), `carriers` the subset whose
                // effective row still carries the label (re-added) — the exact split
                // `fold_label_scan` performs.
                let mut exclude: HashSet<u64> = HashSet::new();
                let mut overlay: Vec<u64> = Vec::new();
                if !stack.is_singleton() {
                    if let Some(label) = label.as_deref() {
                        if let Some((touched, carriers)) = stack.label_scan_overlay(label)? {
                            exclude = touched.into_iter().collect();
                            overlay = carriers;
                        }
                    }
                }
                // Delta-born nodes (Phase 2c) are not in the base label column; ids that
                // *gained* the label via `SET n:Label` are re-added here too. A core node
                // that *dropped* the label stays in the base sweep and is re-checked and
                // rejected by `node_ok` (the scan is not trusted to prove the label — see
                // `scan_guaranteed_labels`); tombstoned ids are dropped by per-window
                // suppression. The empty-delta fast path skips the lookup entirely.
                if !delta.is_empty() {
                    if let Some(label) = label.as_deref() {
                        overlay.extend(delta.born_ids_with_label(label));
                        overlay.extend(delta.ids_with_added_label(label));
                    }
                }
                overlay.sort_unstable();
                overlay.dedup();
                let mut srcs = vec![MergeSrc::LabelCol {
                    label: *label_id,
                    exclude,
                    col_end: self.gen.node_label_column_len(),
                }];
                if !overlay.is_empty() {
                    srcs.push(MergeSrc::Mat {
                        ids: overlay,
                        pos: 0,
                    });
                }
                return Ok(CandidateStream::merge(srcs, self.gen.node_count()));
            }
            // Distinct edge-having endpoint nodes for the typed first hop (HIK-104). The
            // base posting expands to the whole endpoint set (733 MB for a dense reltype),
            // so stream it: each base posting is a lazy `Posting` cursor over the compressed
            // Elias–Fano form, each segment's endpoint slice a write-bounded `Mat` source, and
            // the k-way merge produces the ascending+deduped union a window at a time. Postings
            // carry no removals — a superset stays correct because the first hop re-filters by
            // reltype (and per-window suppression drops deleted nodes).
            NodeScan::RelTypeScan {
                reltype_ids, side, ..
            } => {
                let mut srcs: Vec<MergeSrc> = self
                    .gen
                    .endpoint_posting_cursors(reltype_ids, *side)?
                    .into_iter()
                    .map(|mut iter| {
                        let head = iter.next();
                        MergeSrc::Posting { iter, head }
                    })
                    .collect();
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    for seg in stack.segments() {
                        let Some(post) = &seg.postings else { continue };
                        for &rt in reltype_ids {
                            let Some(name) = self.gen.reltype_name(rt) else {
                                continue;
                            };
                            let mut seg_ids: Vec<u64> = Vec::new();
                            if matches!(side, RelEndpointSide::Source | RelEndpointSide::Either) {
                                seg_ids.extend_from_slice(post.src_ids(name));
                            }
                            if matches!(side, RelEndpointSide::Target | RelEndpointSide::Either) {
                                seg_ids.extend_from_slice(post.tgt_ids(name));
                            }
                            if !seg_ids.is_empty() {
                                // A segment's src and tgt slices are each ascending+distinct,
                                // but their concatenation (Either side) is not — normalise so
                                // the merge's per-source ascending invariant holds.
                                seg_ids.sort_unstable();
                                seg_ids.dedup();
                                srcs.push(MergeSrc::Mat {
                                    ids: seg_ids,
                                    pos: 0,
                                });
                            }
                        }
                    }
                }
                return Ok(CandidateStream::merge(srcs, self.gen.node_count()));
            }
        };
        let mut ids = ids;
        self.suppress_tombstoned_in_place(&mut ids)?;
        self.scanned_ids
            .set(self.scanned_ids.get().saturating_add(ids.len() as u64));
        Ok(CandidateStream::owned(ids))
    }

    /// The next window of candidate ids from `s`, or `None` once the scan is exhausted.
    /// The slice borrows the stream and stays valid until the next call.
    ///
    /// A lazy window is decoded, tombstone-suppressed and counted into
    /// [`Engine::anchor_ids_scanned`] as it is produced — so a consumer that stops early
    /// (a pushed `LIMIT`) leaves the rest of the id space untouched. A window that
    /// suppression (or the label filter) empties is skipped, not yielded, so a caller never
    /// mistakes it for the end of the scan.
    pub(crate) fn next_candidates<'s>(
        &self,
        s: &'s mut CandidateStream<'_>,
    ) -> Result<Option<&'s [u64]>> {
        let CandidateStream {
            src,
            pos,
            buf,
            window,
        } = s;
        let (label, next, end) = match src {
            // Materialised sources are handed out in windows too, so a capped consumer
            // walks no further into them than it must. They are already suppressed and
            // counted, so no per-window work is done here.
            CandidateSrc::Ready(ids) => return Ok(slice_window(ids, pos)),
            CandidateSrc::Owned(ids) => return Ok(slice_window(ids, pos)),
            CandidateSrc::Merge { srcs, next, end } => {
                return self.next_merge_window(srcs, next, end, buf, window);
            }
            CandidateSrc::Sweep { label, next, end } => (label, next, end),
        };
        buf.clear();
        while *next < *end {
            let (lo, hi) = (*next, (*next + *window).min(*end));
            *next = hi;
            // Ramp the window: the first one is small so a `LIMIT 1` touches ~1 K ids,
            // then it grows to `CAND_WINDOW_MAX` so an uncapped sweep amortises the
            // per-window block locate/decompress back to the old single-pass cost.
            *window = window.saturating_mul(8).min(CAND_WINDOW_MAX);
            // A long sweep is now interruptible: the eager collect ran to completion before
            // the executor could look at the clock again.
            self.check_deadline()?;
            self.scanned_ids
                .set(self.scanned_ids.get().saturating_add(hi - lo));
            match label {
                None => buf.extend(lo..hi),
                Some(l) => {
                    let labels = self.gen.node_labels();
                    let (bitmask, want) = (labels.bitmask(), *l);
                    labels.inner().for_each_record_in(lo, hi, |node_id, rec| {
                        if graph_format::nodelabels::decode_labels(rec, bitmask)?.contains(&want) {
                            buf.push(node_id);
                        }
                        Ok(())
                    })?;
                }
            }
            self.suppress_tombstoned_in_place(buf)?;
            if !buf.is_empty() {
                return Ok(Some(buf.as_slice()));
            }
        }
        Ok(None)
    }

    /// Drain the next non-empty window of a k-way anchor merge ([`CandidateSrc::Merge`],
    /// HIK-104). It partitions the id space into the *same* ramping windows a sweep uses;
    /// per window `[lo, hi)` it pulls only the ids each source has in range — the base label
    /// column decoded lazily (minus the stack-overridden ids), each base endpoint posting
    /// walked from its compressed Elias–Fano cursor, and the write-bounded overlays advanced
    /// by index — then `sort_unstable`+`dedup`s that bounded buffer. Every id falls in exactly
    /// one window, so this reproduces the eager path's global `sort`+`dedup` ordering and
    /// cross-source dedup exactly while the resident footprint stays one window. Tombstones are
    /// suppressed per window (a `SET`-dropped label is left to `node_ok`, as before), the
    /// id-space frontier walked is counted into [`Engine::anchor_ids_scanned`] so a pushed
    /// `LIMIT` stops the merge, and empty windows are skipped rather than yielded.
    pub(crate) fn next_merge_window<'b>(
        &self,
        srcs: &mut [MergeSrc],
        next: &mut u64,
        end: &mut u64,
        buf: &'b mut Vec<u64>,
        window: &mut u64,
    ) -> Result<Option<&'b [u64]>> {
        buf.clear();
        while *next < *end {
            let (lo, hi) = (*next, (*next + *window).min(*end));
            *next = hi;
            *window = window.saturating_mul(8).min(CAND_WINDOW_MAX);
            self.check_deadline()?;
            self.scanned_ids
                .set(self.scanned_ids.get().saturating_add(hi - lo));
            for src in srcs.iter_mut() {
                match src {
                    // Write-bounded, already ascending+distinct: emit its ids below `hi`. The
                    // cursor is monotone, so everything left is `>= lo` already.
                    MergeSrc::Mat { ids, pos } => {
                        while *pos < ids.len() && ids[*pos] < hi {
                            buf.push(ids[*pos]);
                            *pos += 1;
                        }
                    }
                    // Ascending Elias–Fano posting cursor: emit its ids below `hi`, keeping the
                    // straddling head for the window it belongs to.
                    MergeSrc::Posting { iter, head } => {
                        while let Some(h) = *head {
                            if h < hi {
                                buf.push(h);
                                *head = iter.next();
                            } else {
                                break;
                            }
                        }
                    }
                    // The base label column, decoded lazily over this window (clamped to the
                    // column's records; higher ids are born ids the overlay supplies), keeping
                    // the label's carriers that the stack does not override.
                    MergeSrc::LabelCol {
                        label,
                        exclude,
                        col_end,
                    } => {
                        let chi = hi.min(*col_end);
                        if lo < chi {
                            let labels = self.gen.node_labels();
                            let (bitmask, want) = (labels.bitmask(), *label);
                            labels.inner().for_each_record_in(lo, chi, |node_id, rec| {
                                if graph_format::nodelabels::decode_labels(rec, bitmask)?
                                    .contains(&want)
                                    && !exclude.contains(&node_id)
                                {
                                    buf.push(node_id);
                                }
                                Ok(())
                            })?;
                        }
                    }
                }
            }
            buf.sort_unstable();
            buf.dedup();
            self.suppress_tombstoned_in_place(buf)?;
            if !buf.is_empty() {
                return Ok(Some(buf.as_slice()));
            }
        }
        Ok(None)
    }

    /// Candidate node ids for a chosen scan strategy, **materialised**. The eager
    /// counterpart of [`Engine::candidate_stream`], for the consumers that genuinely need
    /// the whole set at once: the `algo.*` subgraph view (it builds an index over it), the
    /// indexed count fast path (it wants only the length), a candidate set hoisted across
    /// many input rows, and the scan-seam tests.
    pub(crate) fn scan_candidates(&self, scan: &NodeScan) -> Result<Vec<u64>> {
        let mut s = self.candidate_stream(scan)?;
        // An eagerly-materialised source is already the answer.
        if let CandidateSrc::Owned(ids) = s.src {
            return Ok(ids);
        }
        let mut out = Vec::new();
        while let Some(batch) = self.next_candidates(&mut s)? {
            out.extend_from_slice(batch);
        }
        Ok(out)
    }

    /// Drop candidate dense ids a deletion has tombstoned — the delta's (Phase 2) *and* the
    /// core stack's (a flush that deleted a node): a deleted node must never bind as an
    /// anchor. The pure-core singleton with an empty delta returns the input untouched, so
    /// the read-only path pays nothing. In place, so a scan window is filtered without a
    /// second allocation.
    pub(crate) fn suppress_tombstoned_in_place(&self, ids: &mut Vec<u64>) -> Result<()> {
        let delta = self.gen.delta();
        let stack = self.gen.core_stack();
        if delta.is_empty() && stack.is_singleton() {
            return Ok(());
        }
        let (mut keep, mut i) = (0usize, 0usize);
        while i < ids.len() {
            let id = ids[i];
            i += 1;
            if delta.is_tombstoned(id) || (!stack.is_singleton() && stack.is_node_tombstoned(id)?) {
                continue;
            }
            ids[keep] = id;
            keep += 1;
        }
        ids.truncate(keep);
        Ok(())
    }

    /// The `(label, property)` a node range index is defined on, for the delta-born
    /// overlay (Phase 2d): a born node enters index `index` only if it carries
    /// `label` and its `property` value satisfies the seek. `None` if the name is
    /// not an open node range index.
    pub(crate) fn node_index_label_prop(&self, index: &str) -> Option<(&str, &str)> {
        self.gen
            .manifest()
            .range_indexes
            .iter()
            .find(|ri| ri.name == index && ri.entity == EntityKind::Node)
            .map(|ri| (ri.label_or_type.as_str(), ri.property.as_str()))
    }

    /// The label ids a chosen anchor scan already proves every candidate carries,
    /// so `node_ok` can skip re-decoding a label record for them (root cause 2). A
    /// `LabelScan` proves its label; a range-index scan proves the (node) label the
    /// index is defined on — a node only enters that index if it carries that label.
    /// Id seeks and full scans prove nothing.
    pub(crate) fn scan_guaranteed_labels(&self, scan: &NodeScan) -> Vec<u32> {
        match scan {
            // A label scan proves its label — unless a label mutation is present, in
            // which case a scanned candidate may have dropped it (Stage 5); force
            // `node_ok` to re-check by proving nothing.
            NodeScan::LabelScan { label_id } => {
                if self.gen.delta().has_label_overlay() {
                    Vec::new()
                } else {
                    vec![*label_id]
                }
            }
            NodeScan::RangeEq { index, .. } | NodeScan::RangeRange { index, .. } => self
                .gen
                .manifest()
                .range_indexes
                .iter()
                .find(|ri| &ri.name == index && ri.entity == EntityKind::Node)
                .and_then(|ri| self.gen.label_id(&ri.label_or_type))
                .into_iter()
                .collect(),
            NodeScan::IdSeek { .. } | NodeScan::AllNodes => Vec::new(),
            // The posting proves an *edge*, not a label; carry the anchor's lone
            // required label (lifted from the replaced LabelScan) so `node_ok`
            // still skips that label record, but re-checks anything else.
            NodeScan::RelTypeScan {
                guaranteed_label, ..
            } => guaranteed_label.iter().copied().collect(),
        }
    }

    /// Whether [`Self::node_ok`] would read a per-candidate label or property record
    /// for the anchor `start` — i.e. whether a parallel filter over many scanned
    /// candidates (Task 10) is worth the fan-out. Returns false when the filter is
    /// constant or already proven by the scan: no labels and no inline props, a single
    /// label atom the scan already guaranteed, or an unknown single label (which
    /// rejects every candidate with no record read at all).
    pub(crate) fn anchor_filter_reads(&self, start: &NodePat, guaranteed: &[u32]) -> bool {
        if !start.props.is_empty() {
            return true;
        }
        match &start.label_expr {
            None => false,
            Some(expr) => match expr.as_single_atom() {
                Some(atom) => match self.gen.label_id(atom) {
                    Some(lid) => !guaranteed.contains(&lid),
                    None => false,
                },
                None => true,
            },
        }
    }

    /// Whether node `id` satisfies a node pattern's labels and inline properties.
    /// Inline property values are evaluated against `binding` so a value bound
    /// earlier (e.g. by a `WITH`, or an earlier node/rel in the pattern) resolves,
    /// making `(b {id: x})` behave exactly like `(b) WHERE b.id = x`.
    ///
    /// `guaranteed` lists label ids the caller's anchor scan already proved for `id`
    /// (see [`scan_guaranteed_labels`]); those are skipped so the common
    /// label-scan/index-scan path never decodes a label record. Downstream
    /// (traversal) callers pass `&[]` — their candidates carry no such proof.
    pub(crate) fn node_ok(
        &self,
        id: u64,
        pat: &NodePat,
        scope: &Scope,
        guaranteed: &[u32],
    ) -> Result<bool> {
        if let Some(expr) = &pat.label_expr {
            // Fast path, byte-for-byte as cheap as the pre-GQL single-label check: a
            // lone positive atom `(:Person)` the anchor scan already proved needs no
            // label record at all. This call is hot (one per candidate per hop), so
            // the common case must never touch the label record or, when guaranteed,
            // even the symbol table beyond one lookup.
            if let Some(atom) = expr.as_single_atom() {
                match self.gen.label_id(atom) {
                    Some(lid) if guaranteed.contains(&lid) => {}
                    Some(lid) => {
                        if !self.node_label_ids(id)?.contains(&lid) {
                            return Ok(false);
                        }
                    }
                    None => return Ok(false), // unknown label, single atom ⇒ no match
                }
            } else {
                // A boolean label expression (`&`/`|`/`!`, parens): decode the resident
                // labels once and evaluate as plain set membership. Anchor-proven
                // labels are folded into the present-predicate so a guaranteed atom
                // still counts without re-decoding. An atom naming an unknown label is
                // simply absent — so `!Unknown` holds and `Unknown` fails, the sound
                // set-logic answer.
                let have = self.node_label_ids(id)?;
                let ok = expr.eval(&|name| {
                    self.gen
                        .label_id(name)
                        .is_some_and(|lid| guaranteed.contains(&lid) || have.contains(&lid))
                });
                if !ok {
                    return Ok(false);
                }
            }
        }
        for (k, e) in &pat.props {
            let want = self.eval(e, scope, None)?;
            let got = self.node_prop(id, k)?;
            if got.loose_eq(&want) != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether edge `id` satisfies a relationship pattern's inline properties.
    /// Values are evaluated against `binding` (see [`node_ok`]).
    pub(crate) fn rel_ok(
        &self,
        id: u64,
        rel: &RelPat,
        binding: &HashMap<String, Val>,
    ) -> Result<bool> {
        for (k, e) in &rel.props {
            let want = self.eval(e, &Scope::Map(binding), None)?;
            let got = self.edge_prop(id, k)?;
            if got.loose_eq(&want) != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // ── Projection (RETURN / WITH) ──────────────────────────────────────────
}
