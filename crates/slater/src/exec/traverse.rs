// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for pattern traversal and multi-hop expansion.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Match `patterns[idx..]` against `binding`, applying the clause `WHERE` once
    /// every pattern is bound, collecting completed bindings.
    pub(crate) fn match_patterns(
        &self,
        patterns: &[Pattern],
        idx: usize,
        binding: HashMap<String, Val>,
        where_: Option<&Expr>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        if cap.is_some_and(|c| out.len() >= c) {
            return Ok(());
        }
        if idx == patterns.len() {
            if let Some(w) = where_ {
                if !truthy(&self.eval(w, &Scope::Map(&binding), None)?) {
                    return Ok(());
                }
            }
            // Charging each emitted binding bounds dense-graph materialisation
            // (plain MATCH and pattern comprehensions alike) by the query budget.
            self.charge(1)?;
            out.push(binding);
            return Ok(());
        }
        // The pushed cap (Stage 6) bounds this pattern's own expansion only when it
        // is the LAST pattern AND there is no residual WHERE — then each emitted
        // binding becomes exactly one output row (1:1), so the expansion needs at
        // most `cap - out.len()` rows. Otherwise downstream patterns/WHERE may drop
        // or multiply rows, so the per-pattern walk stays uncapped (only the `out`
        // accumulation below stops early).
        let sp_cap = if idx + 1 == patterns.len() && where_.is_none() {
            cap.map(|c| c.saturating_sub(out.len()))
        } else {
            None
        };
        let mut partial = Vec::new();
        self.match_single_pattern(&patterns[idx], &binding, where_, &mut partial, sp_cap)?;
        for b in partial {
            if cap.is_some_and(|c| out.len() >= c) {
                break;
            }
            self.match_patterns(patterns, idx + 1, b, where_, out, cap)?;
        }
        Ok(())
    }

    pub(crate) fn match_single_pattern(
        &self,
        pattern: &Pattern,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        // If the start anchor would be a full scan but the pattern's *end* node is
        // id-seekable, match the reversed pattern so the seekable node leads. This
        // is what turns Memgraph Lab's `MATCH (m)-[r]->(n) WHERE id(n) = X`
        // neighbourhood-expansion (id pinned on the far end) from a full edge scan
        // into a seek + one-hop walk. Reversal preserves the binding set exactly
        // (same vars, same edges, flipped traversal direction) and the full WHERE
        // is re-checked downstream, so it cannot change results.
        let rerooted = self.maybe_reroot(pattern, binding, where_);
        let pattern = rerooted.as_ref().unwrap_or(pattern);
        let start = &pattern.start;
        // `guaranteed` are the anchor labels the chosen scan already proves for every
        // candidate, so `node_ok` can skip re-decoding a label record for them
        // (root cause 2). Only the scanned branch yields guarantees; an already-bound
        // anchor is a single node we still verify in full.
        let (mut candidates, guaranteed): (CandidateStream, Vec<u32>) =
            match start.var.as_deref().and_then(|v| binding.get(v)) {
                Some(Val::Node(id)) => (CandidateStream::single(*id), Vec::new()),
                Some(_) => return Ok(()), // bound to a non-node → cannot match
                None => {
                    // The anchor is the only place the planner picks a scan strategy.
                    // Scalars already bound for this row let an anchor keyed by a
                    // bound variable (`{p: w}` / `WHERE n.p = w`) seek the index.
                    let bound = bound_scalars(binding);
                    let scan = choose_node_scan(self.gen, start, where_, &self.plan_params, &bound);
                    // If the (post-reroot) pattern's first hop is a required,
                    // fixed-length typed edge, drive from that reltype's endpoint
                    // posting instead of a label/full scan — skipping the nodes
                    // that have no such edge. Sound for any context (incl.
                    // OPTIONAL): only reached for an unbound anchor, and an
                    // edgeless anchor yields no row under either plan, so the
                    // matched set is identical. See `maybe_rel_type_scan`.
                    let scan = maybe_rel_type_scan(self.gen, &scan, pattern).unwrap_or(scan);
                    let guaranteed = self.scan_guaranteed_labels(&scan);
                    (self.candidate_stream(&scan)?, guaranteed)
                }
            };
        // One mutable frame for the whole anchor loop. Each candidate binds the
        // anchor var in place, expands, then restores it — instead of cloning the
        // inherited scope per candidate, and (in `expand_chain`) per hop per
        // neighbour. `node_ok` still sees the pre-anchor scope (the anchor's own
        // var intentionally absent, as before), since `frame` is restored to the
        // base binding between candidates.
        // The chain shape (rels, props, var-length) is the same for every anchor, so
        // decide once whether each anchor's expansion uses the parallel breadth-first
        // walk (Task 9) or the sequential depth-first one. A pushed `LIMIT` (`cap`)
        // disables it: the breadth-first walk would eagerly read a whole hop level
        // before the cap could stop it, over-reading a high-degree frontier the
        // depth-first early-exit would have skipped — so capped chains stay sequential
        // (the plan's early-exit rule). Uncapped chains (counts, aggregates, DISTINCT,
        // un-LIMITed returns) genuinely need the whole neighbourhood, so the parallel
        // reads are pure overlap with no wasted work.
        let parallel = cap.is_none() && self.chain_parallelizable(pattern);
        // Task 10: when the anchor is a scan wide enough to be worth it and `node_ok`
        // actually reads a per-candidate label/property record, evaluate that filter
        // across the shared fanout pool up front, then expand only the survivors in
        // input order. The inline-prop *values* (`wants`) don't depend on the
        // candidate, so they are evaluated once here (single-threaded — they may route
        // through the !Sync evaluator) and the workers do only Sync label/column reads
        // + `loose_eq`. Gated to uncapped scans: a pushed `LIMIT` would over-read the
        // whole candidate set before the cap could stop the scan (the plan's early-exit
        // rule), so capped scans keep the inline per-candidate filter with its break.
        // The scan is a stream now, so the filter runs a window at a time (each window is
        // ≫ `SCAN_PAR_MIN`, so it still fans out) rather than over one giant `Vec`; the
        // width gate reads the stream's upper bound instead of a materialised length.
        let prefilter = cap.is_none()
            && self.fanout_pool.is_some()
            && candidates.upper_bound() >= SCAN_PAR_MIN
            && self.anchor_filter_reads(start, &guaranteed);
        // Candidate-independent, so evaluated once for the whole scan (single-threaded —
        // it may route through the !Sync evaluator); the pool workers then do only Sync
        // label/column reads + `loose_eq`.
        let wants: Vec<(&str, Val)> = if prefilter {
            start
                .props
                .iter()
                .map(|(k, e)| Ok((k.as_str(), self.eval(e, &Scope::Map(binding), None)?)))
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };
        // Degree-sum terminal: in count-pushdown mode (armed by `count_match`, no WHERE),
        // if this post-reroot pattern's final hop qualifies, arm the walk to add each
        // penultimate node's effective degree instead of expanding the last relationship.
        // Checked on the *post-reroot* pattern, so a rerooted chain (whose new terminal is
        // the filtered original anchor) declines automatically. Restored after the loop.
        let degree_term = self.count_acc.get().is_some()
            && where_.is_none()
            && self.degree_terminal_dir(pattern).is_some();
        let prev_degree_term = self.degree_terminal.replace(degree_term);
        let mut frame = binding.clone();
        let mut walk = Vec::new();
        // Filter verdicts for the current window (`prefilter` only), reused across windows.
        let mut pass: Vec<bool> = Vec::new();
        'scan: while let Some(batch) = self.next_candidates(&mut candidates)? {
            if prefilter {
                let (gen, cache) = (self.gen, self.cache);
                let label_expr = start.label_expr.as_ref();
                pass = par_gather(self.fanout_pool.as_deref(), batch, SCAN_PAR_MIN, |&c| {
                    node_ok_par(gen, cache, c, label_expr, &wants, &guaranteed)
                })?;
            }
            for (i, &c) in batch.iter().enumerate() {
                // Stage 6: once a pushed `LIMIT` is met, stop scanning anchors — the
                // remaining candidates can only add rows the projection would truncate,
                // and the stream stops producing them.
                if cap.is_some_and(|cc| out.len() >= cc) {
                    break 'scan;
                }
                // Already filtered in parallel above when `prefilter`; otherwise check the
                // anchor's labels/inline props inline (with the loop's early-exit break).
                if prefilter {
                    if !pass[i] {
                        continue;
                    }
                } else if !self.node_ok(c, start, &Scope::Map(&frame), &guaranteed)? {
                    continue;
                }
                let prev = start
                    .var
                    .as_ref()
                    .map(|v| (v.clone(), frame.insert(v.clone(), Val::Node(c))));
                if parallel {
                    self.expand_chain_par(pattern, c, &frame, out, cap)?;
                } else {
                    debug_assert!(walk.is_empty());
                    self.expand_chain(pattern, 0, c, &mut frame, c, &mut walk, out, cap)?;
                }
                if let Some((v, old)) = prev {
                    restore_binding(&mut frame, v, old);
                }
            }
        }
        self.degree_terminal.set(prev_degree_term);
        Ok(())
    }

    /// Decide whether to match `pattern` reversed so a concrete (single-candidate)
    /// **end** node leads instead of a full-scan start. Returns the reversed pattern
    /// when it would help, else `None` (match the original). Reversal preserves the
    /// binding set exactly (same vars, same edges, flipped traversal direction) and
    /// the full WHERE is re-checked downstream, so it cannot change results.
    ///
    /// Common preconditions: the pattern has at least one relationship and **no**
    /// variable-length hop (a reversed `*` walk could reorder a returned
    /// relationship list); it has no path variable (reversal would reverse the
    /// path); and the **start** is a fresh scan (an already-bound start leads with a
    /// concrete node, so reversal could only lose that).
    ///
    /// Two cases re-root:
    /// - **(1) end already bound** by an outer `MATCH`/`WITH` to a concrete node —
    ///   lead with that node and walk its reverse adjacency, instead of full-scanning
    ///   the start label once per bound end row. This is the eu-ai-act §P1
    ///   reverse-traversal case: `… MATCH (c:Chunk)-[:SOURCED_FROM]->(b)` with `b`
    ///   bound went from a seek to an O(|Chunk|)-per-row scan without it.
    /// - **(2) end id-anchored by `WHERE`** (`… WHERE id(end) = X`, the start *not*
    ///   anchored) — seek the end and walk back, turning a full edge scan into a
    ///   seek + one-hop (Memgraph Lab neighbourhood expansion).
    pub(crate) fn maybe_reroot(
        &self,
        pattern: &Pattern,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
    ) -> Option<Pattern> {
        if pattern.path_var.is_some() || pattern.rels.is_empty() {
            return None;
        }
        if pattern.rels.iter().any(|(r, _)| r.var_length.is_some()) {
            return None;
        }
        let unbound = |v: Option<&String>| v.is_none_or(|name| !binding.contains_key(name));
        if !unbound(pattern.start.var.as_ref()) {
            return None;
        }
        let end = &pattern.rels.last().unwrap().1;
        let end_var = end.var.as_deref()?;
        // (1) End node already bound to a concrete node — lead with it.
        if matches!(binding.get(end_var), Some(Val::Node(_))) {
            return Some(reverse_pattern(pattern));
        }
        // (2) End must otherwise be a fresh scan target id-anchored by WHERE.
        if !unbound(end.var.as_ref()) {
            return None;
        }
        let where_ = where_?;
        let start_anchored = pattern
            .start
            .var
            .as_deref()
            .is_some_and(|v| is_id_anchored(where_, v));
        if start_anchored || !is_id_anchored(where_, end_var) {
            return None;
        }
        Some(reverse_pattern(pattern))
    }

    /// Whether `pattern`'s chain qualifies for the parallel breadth-first expansion
    /// ([`Self::expand_chain_par`], Task 9): a fanout pool is configured and the
    /// pattern is a plain (non-quantified) chain of at least one **fixed-length,
    /// property-free** relationship. Property-bearing rels need `rel_ok` (which calls
    /// the `!Sync` evaluator) and variable-length rels recurse through `varlen`
    /// (which charges the budget mid-recursion); both stay on the sequential
    /// [`Self::expand_chain`] path. Node-side labels/props are unrestricted — they are
    /// re-checked single-threaded in the merge.
    pub(crate) fn chain_parallelizable(&self, pattern: &Pattern) -> bool {
        self.fanout_pool.is_some()
            && pattern.segments.is_none()
            && !pattern.rels.is_empty()
            && pattern
                .rels
                .iter()
                .all(|(r, _)| r.var_length.is_none() && r.props.is_empty())
    }

    /// A conservative **upper bound** on node `node`'s overlaid degree in direction
    /// `dir` — used to route a hub into the streaming reader *before* its adjacency is
    /// materialised. It never under-counts a real hub: the core term is exact, the
    /// segment-born and delta-born terms are added, and deletions/tombstones (which only
    /// *reduce* degree) are ignored — so an over-estimate at worst over-streams, never
    /// OOMs by mistaking a hub for a normal node. A tombstoned node has degree 0.
    ///
    /// The core term is an O(1), zero-I/O lookup in the build-side hub-degree sidecar
    /// (`hub_degrees.blk`) when present; a generation built before the sidecar falls back
    /// to reading the record's leading edge count (one cached block). The segment-born
    /// and delta-born terms are always the same bounded reads.
    pub(crate) fn effective_degree_ub(&self, node: u64, dir: Direction) -> Result<u64> {
        let gen = self.gen;
        if gen.delta().is_tombstoned(node) {
            return Ok(0);
        }
        let one = |outgoing: bool| -> Result<u64> {
            // Core degree. A delta-born id (≥ core node count) has no core record ⇒ 0.
            // With the hub-degree sidecar (new builds): an O(1) lookup — exact for a
            // listed hub, else the node is below the build floor, so its UB is `floor-1`
            // (never under-counts). Without a sidecar (older generation): read the
            // record's leading edge count (one cached block, no full decode).
            let core = if node >= gen.core_generation().node_count() {
                0
            } else {
                let cg = gen.core_generation();
                match cg.hub_degree_floor() {
                    Some(floor) => {
                        let listed = if outgoing {
                            cg.core_out_degree_if_hub(node)
                        } else {
                            cg.core_in_degree_if_hub(node)
                        };
                        listed.unwrap_or(floor.saturating_sub(1) as u64)
                    }
                    None => {
                        let topo = gen.topology();
                        let global = if outgoing {
                            topo.outgoing_global(NodeId(node))
                        } else {
                            topo.incoming_global(NodeId(node))
                        };
                        let rec = self.cache.record(
                            topo.inner(),
                            gen.uuid(),
                            FileKind::Topology,
                            global,
                        )?;
                        topology::adj_count(&rec)?
                    }
                }
            };
            // Segment-born upper bound: the O(#segments), zero-I/O per-segment hub-degree
            // delta fold (Component 2). `max(0, Δ)` — a net-negative segment contribution
            // (more removed than born) is treated as 0, so the bound never deflates below
            // the core term. Zero for a singleton stack.
            let stack = gen.core_stack();
            let seg_delta = if outgoing {
                stack.hub_out_degree_delta(node)
            } else {
                stack.hub_in_degree_delta(node)
            };
            let seg = seg_delta.max(0) as u64;
            // Delta-born (bounded by the byte-capped delta): count live born edges.
            let delta = gen.delta();
            let dlt = if delta.is_empty() {
                0
            } else {
                let edges = if outgoing {
                    delta.out_edges(node)
                } else {
                    delta.in_edges(node)
                };
                edges
                    .iter()
                    .filter(|e| e.edge_id.is_some() && !e.tombstoned)
                    .count() as u64
            };
            Ok(core + seg + dlt)
        };
        Ok(match dir {
            Direction::Outgoing => one(true)?,
            Direction::Incoming => one(false)?,
            Direction::Undirected => one(true)? + one(false)?,
        })
    }

    /// Whether node `node` should be streamed rather than materialised for a hop in
    /// direction `dir` — its [`Self::effective_degree_ub`] is at/above the engine's
    /// `adj_stream_threshold`.
    pub(crate) fn is_hub(&self, node: u64, dir: Direction) -> Result<bool> {
        Ok(self.effective_degree_ub(node, dir)? >= self.adj_stream_threshold)
    }

    /// The **exact** count of `node`'s incident edges in `dir` — the degree-sum terminal's
    /// per-node contribution (see [`Self::degree_terminal`]). Composed from the maintained
    /// degree marginals, never a full adjacency read: core degree (O(1) from the hub-degree
    /// sidecar, else the CSR record's leading count), plus each segment's fence-gated
    /// fragment (born − removed), plus the live delta (born − suppressed).
    ///
    /// Exact **only** under the [`Self::degree_terminal_dir`] preconditions — a homogeneous
    /// final hop (so every incident edge counts) and no pending live node-deletes (so no
    /// non-local tombstone correction is owed). The caller guarantees both before arming.
    pub(crate) fn effective_incident_count(&self, node: u64, dir: Direction) -> Result<u64> {
        match dir {
            Direction::Outgoing => self.directed_edge_count(node, true),
            Direction::Incoming => self.directed_edge_count(node, false),
            Direction::Undirected => Ok(
                self.directed_edge_count(node, true)? + self.directed_edge_count(node, false)?
            ),
        }
    }

    /// Exact effective out-degree (`outgoing`) or in-degree of `node`, composed across the
    /// write path. See [`Self::effective_incident_count`] for the exactness preconditions.
    pub(crate) fn directed_edge_count(&self, node: u64, outgoing: bool) -> Result<u64> {
        let gen = self.gen;
        let cg = gen.core_generation();
        let mut deg: i64 = 0;
        // Core: exact out/in degree. Consult the **pinned** hub sidecar first (O(1), few MB,
        // always resident, covers exactly the mega-hubs) so a hub's degree — which dominates
        // count magnitude — never faults a chunk of the chunk-lazy dense column; then the dense
        // per-node column (O(1) on a resident chunk, else one ~1 MiB chunk fault covering the
        // next 262 K ids) for the long tail; then the record's leading edge count (one cached
        // block, no decode) for a generation with neither. All three are the exact core degree
        // — the sidecar and dense column agree on a listed hub — so the order is answer-neutral,
        // only cheaper. 0 for a delta-born id.
        if node < cg.node_count() {
            let listed = if outgoing {
                cg.core_out_degree_if_hub(node)
            } else {
                cg.core_in_degree_if_hub(node)
            };
            deg += match listed {
                Some(d) => d as i64,
                None => {
                    let dense = if outgoing {
                        cg.node_out_degree(node)
                    } else {
                        cg.node_in_degree(node)
                    };
                    match dense {
                        Some(d) => d as i64,
                        None => {
                            let topo = gen.topology();
                            let global = if outgoing {
                                topo.outgoing_global(NodeId(node))
                            } else {
                                topo.incoming_global(NodeId(node))
                            };
                            let rec = self.cache.record(
                                topo.inner(),
                                gen.uuid(),
                                FileKind::Topology,
                                global,
                            )?;
                            topology::adj_count(&rec)? as i64
                        }
                    }
                }
            };
        }
        // Segments: fence-gated fragment, born (+1) − removed (−1). Bounded per node; an
        // untouched node skips the segment via its O(1) presence fence. Exact composition.
        let stack = gen.core_stack();
        if !stack.is_singleton() {
            for seg in stack.segments() {
                let r = &seg.reader;
                let frag = if outgoing {
                    if !r.may_hold_out_adj(node) {
                        continue;
                    }
                    r.out_adj(node)?
                } else {
                    if !r.may_hold_in_adj(node) {
                        continue;
                    }
                    r.in_adj(node)?
                };
                for e in frag {
                    if e.removed {
                        deg -= 1;
                    } else {
                        deg += 1;
                    }
                }
            }
        }
        // Live delta: born edge (+1), suppressed core edge (−1). Node-tombstones are ruled
        // out upfront (`degree_terminal_dir`), so no edge-to-deleted-node correction is owed.
        let delta = gen.delta();
        if !delta.is_empty() {
            let edges = if outgoing {
                delta.out_edges(node)
            } else {
                delta.in_edges(node)
            };
            for e in edges {
                if e.tombstoned {
                    deg -= 1;
                } else if e.edge_id.is_some() {
                    deg += 1;
                }
            }
        }
        Ok(deg.max(0) as u64)
    }

    /// Whether `pattern`'s final hop is a plain, unfiltered `count`-only edge that can be
    /// answered by summing effective degree over the penultimate frontier instead of
    /// expanding — returning that hop's [`Direction`] when so, else `None` (walk normally).
    ///
    /// Requires: an ordinary fixed-length chain (no path var / quantified segments /
    /// selector / restrictor / variable-length hop); the final relationship carries no
    /// property predicate and its type filter counts **every** incident edge (untyped, or
    /// the graph has exactly one reltype the filter accepts, so degree == matching count);
    /// the final node is unfiltered and not a back-reference to an earlier-bound variable
    /// (which would restrict which endpoints count); and no live node-delete is pending
    /// (which would make a maintained degree non-exact — see [`Self::directed_edge_count`]).
    /// Intermediate hops may be typed/filtered — they are walked normally; only the final
    /// hop is replaced.
    pub(crate) fn degree_terminal_dir(&self, pattern: &Pattern) -> Option<Direction> {
        if pattern.path_var.is_some()
            || pattern.segments.is_some()
            || pattern.selector.is_some()
            || pattern.restrictor.is_some()
            || pattern.rels.is_empty()
            || pattern.rels.iter().any(|(r, _)| r.var_length.is_some())
        {
            return None;
        }
        let (last_rel, last_node) = pattern.rels.last().unwrap();
        if !last_rel.props.is_empty() {
            return None;
        }
        // Final-hop type must count every incident edge: untyped, or a single-reltype graph
        // whose lone type the filter accepts (then out-degree == the matching-edge count).
        match &last_rel.type_expr {
            None => {}
            Some(e) => {
                let reltypes = &self.gen.manifest().reltypes;
                if reltypes.len() != 1 || !e.eval(&|name| name == reltypes[0]) {
                    return None;
                }
            }
        }
        // Final node unfiltered and a fresh endpoint (no cycle constraint).
        if last_node.label_expr.is_some() || !last_node.props.is_empty() {
            return None;
        }
        if let Some(v) = last_node.var.as_deref() {
            let bound_earlier = pattern.start.var.as_deref() == Some(v)
                || pattern.rels[..pattern.rels.len() - 1]
                    .iter()
                    .any(|(r, n)| r.var.as_deref() == Some(v) || n.var.as_deref() == Some(v));
            if bound_earlier {
                return None;
            }
        }
        if self.gen.delta().has_tombstones() {
            return None;
        }
        Some(last_rel.dir)
    }

    /// The per-neighbour merge body of [`Self::par_walk`], factored out so both the
    /// parallel-gathered normal nodes and the sequentially-**streamed** hub nodes share
    /// it verbatim: `node_ok`, the next-var equality guard, the (structurally shared)
    /// binding layer, the path-walk track, and the `EXPAND_BATCH`-bounded depth-first
    /// flush into the next hop. Consumes `hop` (it moves into the branch's `walk`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn walk_merge_hop(
        &self,
        pattern: &Pattern,
        i: usize,
        start: u64,
        rel: &RelPat,
        next: &NodePat,
        track_walk: bool,
        b: &ChainBranch,
        hop: Hop,
        pending: &mut Vec<ChainBranch>,
        out: &mut Vec<HashMap<String, Val>>,
    ) -> Result<()> {
        let nb = hop.neighbour;
        if !self.node_ok(nb, next, &Scope::Frame(&b.binding), &[])? {
            return Ok(());
        }
        if let Some(v) = &next.var {
            if let Some(existing) = b.binding.get(v) {
                if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                    return Ok(());
                }
            }
        }
        // Relationship-uniqueness (openCypher relationship-isomorphism): a multi-hop
        // chain must not reuse an edge already bound earlier in the same walk. `track_walk`
        // is set for such chains (see `par_walk`), so `b.walk` holds the prior hops; the
        // scan is O(chain length). This rejects exactly the hops the sequential
        // `expand_chain` rejects, so leaf order and the charge sequence stay identical.
        if track_walk && b.walk.iter().any(|h| h.edge == hop.edge) {
            return Ok(());
        }
        // Structural share: a hop that binds no variable carries the parent frame
        // unchanged (an `Arc` bump); a binding hop layers a small delta over it.
        let binding = if rel.var.is_none() && next.var.is_none() {
            b.binding.clone()
        } else {
            let mut delta: Vec<(Box<str>, Val)> = Vec::with_capacity(2);
            if let Some(v) = &rel.var {
                delta.push((v.as_str().into(), hop.as_rel()));
            }
            if let Some(v) = &next.var {
                delta.push((v.as_str().into(), Val::Node(nb)));
            }
            std::sync::Arc::new(Frame {
                parent: Some(b.binding.clone()),
                delta,
            })
        };
        let walk = if track_walk {
            let mut w = b.walk.clone();
            w.push(hop);
            w
        } else {
            Vec::new()
        };
        pending.push(ChainBranch {
            cur: nb,
            binding,
            walk,
        });
        // Flush a full batch into the next hop immediately (depth-first on an in-order
        // prefix) so the live frontier never exceeds one batch.
        if pending.len() >= EXPAND_BATCH {
            let batch = std::mem::take(pending);
            self.par_walk(pattern, i + 1, start, batch, out)?;
        }
        Ok(())
    }

    /// Parallel counterpart to [`Self::expand_chain`] for a fixed-length,
    /// property-free chain (gated by [`Self::chain_parallelizable`]). Walks the chain
    /// from anchor `cur` in **bounded breadth batches** ([`Self::par_walk`]): each
    /// batch's adjacency reads gather on the shared fanout pool ([`hops_par`]), then
    /// the merge runs **single-threaded in input order** — `node_ok` + next-var
    /// binding checks, the intermediate budget `charge()`, and the path binding. Only
    /// the adjacency I/O overlaps.
    ///
    /// Batches are expanded depth-first (an in-order prefix of the frontier, fully
    /// expanded before the next), so the emitted rows, their order, and the charge
    /// sequence are byte-for-byte identical to the sequential depth-first walk — while
    /// live memory stays bounded by `EXPAND_BATCH × chain length` instead of the whole
    /// exponential frontier. That bound is what keeps a dense chain failing *cleanly*
    /// at `maxIntermediate` (charged at completion, exactly as `expand_chain`) rather
    /// than ballooning RSS before the first charge.
    pub(crate) fn expand_chain_par(
        &self,
        pattern: &Pattern,
        cur: u64,
        base: &HashMap<String, Val>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        // Only entered for uncapped chains (see `match_single_pattern`): a pushed
        // `LIMIT` would over-read the breadth batches, so it routes to the sequential
        // early-exit path instead.
        debug_assert!(
            cap.is_none(),
            "expand_chain_par must not be used with a pushed cap"
        );
        let init = vec![ChainBranch {
            cur,
            binding: Frame::root(base),
            walk: Vec::new(),
        }];
        self.par_walk(pattern, 0, cur, init, out)
    }

    /// Expand hop `i` of the chain for the in-order branch `frontier`, recursing into
    /// hop `i+1`. See [`Self::expand_chain_par`] for the invariants. `start` is the
    /// anchor node (constant down the recursion, for `make_path`). Reads the frontier
    /// in [`EXPAND_READ_CHUNK`]-node chunks (parallel adjacency reads, freed per
    /// chunk), builds the next-level branches in order, and recurses depth-first as
    /// soon as a batch of [`EXPAND_BATCH`] accumulates — bounding both the read buffer
    /// and the live frontier while preserving depth-first leaf order.
    pub(crate) fn par_walk(
        &self,
        pattern: &Pattern,
        i: usize,
        start: u64,
        frontier: Vec<ChainBranch>,
        out: &mut Vec<HashMap<String, Val>>,
    ) -> Result<()> {
        // Degree-sum terminal: at the last hop, add each penultimate node's effective
        // degree to the count instead of expanding the final (widest) relationship. Armed
        // only in count mode over a qualifying pattern (see `degree_terminal_dir`).
        if self.degree_terminal.get() && i + 1 == pattern.rels.len() {
            let dir = pattern.rels[i].0.dir;
            for b in &frontier {
                self.check_deadline()?;
                // One unit of walk-work per penultimate node — the degree lookup that
                // replaces expanding its final edges. The count itself (`d`) is *not*
                // charged: the fast path's whole point is to tally a huge final hop in
                // O(1), so the traversal to build this frontier is what `maxScan` bounds.
                self.charge_walk(1)?;
                let d = self.effective_incident_count(b.cur, dir)?;
                let n = self.count_acc.get().unwrap_or(0);
                self.count_acc.set(Some(n + d));
            }
            return Ok(());
        }
        if i == pattern.rels.len() {
            // Completion: charge + emit each branch in order (mirrors `expand_chain`'s
            // terminal — one intermediate per emitted row, path bound if requested).
            for b in frontier {
                self.charge_walk(1)?;
                // Count-pushdown: tally the row and skip building it (no flatten, no
                // alloc) — the whole point of the fast path.
                if self.count_tally() {
                    continue;
                }
                // The owned map every downstream consumer expects is built here, once
                // per completed row — the only flatten in the walk.
                let mut binding = b.binding.flatten();
                if let Some(pv) = &pattern.path_var {
                    binding.insert(pv.clone(), make_path(start, &b.walk));
                }
                out.push(binding);
            }
            return Ok(());
        }
        let (gen, cache) = (self.gen, self.cache);
        let (rel, next) = &pattern.rels[i];
        let tf = resolve_type_filter(gen, rel);
        let dir = rel.dir;
        // Track the per-branch walk when a path variable needs it, OR when the chain
        // has more than one relationship — a multi-hop chain needs the prior hops'
        // edge ids to enforce relationship-uniqueness in `walk_merge_hop`. A single-hop
        // chain has no earlier edge to collide with, so it stays allocation-free.
        let track_walk = pattern.path_var.is_some() || pattern.rels.len() > 1;
        let mut pending: Vec<ChainBranch> = Vec::new();
        // Read in small node-chunks, not the whole frontier at once: a chunk's
        // adjacency buffer (`neigh`) is freed before the next is read, so live read
        // memory stays `O(EXPAND_READ_CHUNK × degree)` — one chunk's worth — instead
        // of the whole frontier's edges. Without this, a frontier of high-degree hubs
        // materialises tens of millions of edges in a single buffer (the sequential
        // walk only ever holds one node's adjacency). The chunk is ≥ [`EXPAND_PAR_MIN`]
        // so each read still fans out across the pool.
        for chunk in frontier.chunks(EXPAND_READ_CHUNK) {
            self.check_deadline()?;
            // Route hub nodes out of the wide parallel gather: a hub's multi-million-edge
            // adjacency materialised inside `par_gather` (up to a whole chunk of them at
            // once) is the fan-out OOM. Decide per node from the cheap upper-bound degree
            // probe; gather the *normal* nodes in parallel as before, and stream each hub
            // sequentially in bounded chunks (its live buffer stays `O(ADJ_STREAM_CHUNK)`).
            let hub: Vec<bool> = chunk
                .iter()
                .map(|b| self.is_hub(b.cur, dir))
                .collect::<Result<_>>()?;
            let normal_nodes: Vec<u64> = chunk
                .iter()
                .zip(&hub)
                .filter(|(_, &h)| !h)
                .map(|(b, _)| b.cur)
                .collect();
            let neigh = par_gather(
                self.fanout_pool.as_deref(),
                &normal_nodes,
                EXPAND_PAR_MIN,
                |&n| hops_par(gen, cache, n, dir, tf.as_ref()),
            )?;
            // Charge the gathered normal hops against the intermediate budget (root cause
            // 2b), mirroring the sequential `expand_one_hop`. `hops_par` runs on the rayon
            // pool where `self.charge` (a non-`Sync` `Cell`) cannot be touched, so the
            // charge stays here on the calling thread once the buffer is materialised.
            // Streamed hub hops are charged per streamed chunk below, as they are produced.
            let produced: u64 = neigh.iter().map(|h| h.len() as u64).sum();
            self.charge_walk(produced)?;
            // Merge in input order — `walk_merge_hop` is the shared per-neighbour body.
            // Normal nodes consume their gathered hops (in chunk order); hub nodes stream.
            let mut normal = neigh.into_iter();
            for (b, &is_hub_node) in chunk.iter().zip(&hub) {
                if is_hub_node {
                    for_each_hop_overlaid(
                        gen,
                        cache,
                        b.cur,
                        dir,
                        tf.as_ref(),
                        self.adj_stream_chunk,
                        &mut |hops| {
                            self.charge_walk(hops.len() as u64)?;
                            for hop in hops {
                                self.walk_merge_hop(
                                    pattern,
                                    i,
                                    start,
                                    rel,
                                    next,
                                    track_walk,
                                    b,
                                    hop.clone(),
                                    &mut pending,
                                    out,
                                )?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    let hops = normal.next().expect("one gather result per normal node");
                    for hop in hops {
                        self.walk_merge_hop(
                            pattern,
                            i,
                            start,
                            rel,
                            next,
                            track_walk,
                            b,
                            hop,
                            &mut pending,
                            out,
                        )?;
                    }
                }
            }
        }
        if !pending.is_empty() {
            self.par_walk(pattern, i + 1, start, pending, out)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // recursive walk: scratch path buffer + start anchor
    pub(crate) fn expand_chain(
        &self,
        pattern: &Pattern,
        i: usize,
        cur: u64,
        binding: &mut HashMap<String, Val>,
        start: u64,
        walk: &mut Vec<Hop>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        // Mutate-in-place binding frame (root cause 6): rather than `binding.clone()`
        // per neighbour per hop, each branch inserts its hop's rel/next bindings,
        // recurses, then restores them on backtrack via `restore_binding`. The only
        // remaining clone is one per *completed* row (when pushing into `out`),
        // which is unavoidable and matches the streaming-scan path. `walk` (the path
        // scratch) and the var-length `used` set already use the same push/pop
        // discipline, so siblings stay isolated.
        //
        // Stage 6: `cap` is the number of rows the final `LIMIT` still needs (only
        // set when the projection is a 1:1 map with no ORDER BY/aggregation/DISTINCT
        // and no residual WHERE — see `match_patterns`). Once `out` reaches it we
        // unwind without expanding further, so a query like the 3-hop that would
        // otherwise buffer ~28k paths for `LIMIT 100` stops after 100.
        if cap.is_some_and(|c| out.len() >= c) {
            return Ok(());
        }
        // Degree-sum terminal (uncapped count fast path): at the last hop, add `cur`'s
        // effective degree to the count instead of expanding the final relationship. See
        // [`Self::degree_terminal`] / [`Self::par_walk`]'s matching short-circuit.
        if self.degree_terminal.get() && i + 1 == pattern.rels.len() {
            // One unit of walk-work (the degree lookup); the count is tallied in O(1) and
            // deliberately not charged to `maxScan` (see `par_walk`'s matching hook).
            self.charge_walk(1)?;
            let d = self.effective_incident_count(cur, pattern.rels[i].0.dir)?;
            let n = self.count_acc.get().unwrap_or(0);
            self.count_acc.set(Some(n + d));
            return Ok(());
        }
        if i == pattern.rels.len() {
            // Charge each completed binding. `match_single_pattern` buffers the whole
            // single-pattern result set here (its `partial` vector) *before* the
            // cross-pattern join re-charges it at the `match_patterns` terminal, so a
            // dense expansion (e.g. every `:LINK` edge over a 1M-node graph) must trip
            // the budget here — otherwise `partial` balloons RSS to an OOM before the
            // charged terminal is ever reached. The double count over the two buffers
            // mirrors their genuine combined peak (conservative on purpose).
            self.charge_walk(1)?;
            // Count-pushdown: tally the row and skip building it (no clone).
            if self.count_tally() {
                return Ok(());
            }
            if let Some(pv) = &pattern.path_var {
                // Bind the path for this completed walk, snapshot the row, then
                // restore so sibling branches don't inherit a stale path value.
                let prev = binding.insert(pv.clone(), make_path(start, walk));
                out.push(binding.clone());
                restore_binding(binding, pv.clone(), prev);
            } else {
                out.push(binding.clone());
            }
            return Ok(());
        }
        self.check_deadline()?;
        let (rel, next) = &pattern.rels[i];
        match &rel.var_length {
            None if cap.is_none() && self.is_hub(cur, rel.dir)? => {
                // Hub source on an uncapped chain: stream its adjacency in bounded chunks
                // rather than materialise the whole hop list (bounds even a lone 10M-edge
                // hub at fan=1). `for_each_hop_overlaid` applies only the type filter, so
                // the relationship-property predicate (`rel_ok`) and the intermediate
                // charge are applied per hop here — matching `expand_one_hop` exactly. The
                // in-place binding/walk push→recurse→restore is unchanged. Gated to
                // `cap.is_none()`: a pushed LIMIT keeps the early-exit materialise path.
                let (gen, cache) = (self.gen, self.cache);
                let tf = resolve_type_filter(gen, rel);
                for_each_hop_overlaid(
                    gen,
                    cache,
                    cur,
                    rel.dir,
                    tf.as_ref(),
                    self.adj_stream_chunk,
                    &mut |hops| {
                        for hop in hops {
                            if !self.rel_ok(hop.edge, rel, binding)? {
                                continue;
                            }
                            self.charge_walk(1)?;
                            let nb = hop.neighbour;
                            if !self.node_ok(nb, next, &Scope::Map(binding), &[])? {
                                continue;
                            }
                            if let Some(v) = &next.var {
                                if let Some(existing) = binding.get(v) {
                                    if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                                        continue;
                                    }
                                }
                            }
                            // Relationship-uniqueness (openCypher relationship-isomorphism):
                            // an edge already bound earlier in this chain cannot be reused
                            // (e.g. an undirected 2-hop bouncing back over the same edge).
                            // `walk` holds this walk's prior hops; scan is O(chain length).
                            if walk.iter().any(|h| h.edge == hop.edge) {
                                continue;
                            }
                            let prev_rel = rel
                                .var
                                .as_ref()
                                .map(|v| (v.clone(), binding.insert(v.clone(), hop.as_rel())));
                            let prev_next = next
                                .var
                                .as_ref()
                                .map(|v| (v.clone(), binding.insert(v.clone(), Val::Node(nb))));
                            walk.push(hop.clone());
                            self.expand_chain(pattern, i + 1, nb, binding, start, walk, out, cap)?;
                            walk.pop();
                            if let Some((v, prev)) = prev_next {
                                restore_binding(binding, v, prev);
                            }
                            if let Some((v, prev)) = prev_rel {
                                restore_binding(binding, v, prev);
                            }
                        }
                        Ok(())
                    },
                )?;
            }
            None => {
                for hop in self.expand_one_hop(cur, rel, binding)? {
                    if cap.is_some_and(|c| out.len() >= c) {
                        break;
                    }
                    let nb = hop.neighbour;
                    if !self.node_ok(nb, next, &Scope::Map(binding), &[])? {
                        continue;
                    }
                    if let Some(v) = &next.var {
                        if let Some(existing) = binding.get(v) {
                            if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                                continue;
                            }
                        }
                    }
                    // Relationship-uniqueness (openCypher relationship-isomorphism): skip a
                    // hop whose edge is already bound earlier in this chain. `walk` holds
                    // this walk's prior hops; the scan is O(chain length).
                    if walk.iter().any(|h| h.edge == hop.edge) {
                        continue;
                    }
                    let prev_rel = rel
                        .var
                        .as_ref()
                        .map(|v| (v.clone(), binding.insert(v.clone(), hop.as_rel())));
                    let prev_next = next
                        .var
                        .as_ref()
                        .map(|v| (v.clone(), binding.insert(v.clone(), Val::Node(nb))));
                    walk.push(hop);
                    self.expand_chain(pattern, i + 1, nb, binding, start, walk, out, cap)?;
                    walk.pop();
                    // Restore LIFO so an aliasing rel/next var name unwinds correctly.
                    if let Some((v, prev)) = prev_next {
                        restore_binding(binding, v, prev);
                    }
                    if let Some((v, prev)) = prev_rel {
                        restore_binding(binding, v, prev);
                    }
                }
            }
            Some(vl) => {
                let (min, max) = varlen_bounds(vl);
                let mode = walk_mode(pattern.restrictor);
                let mut paths: Vec<(Vec<Hop>, u64)> = Vec::new();
                // Seed the trail's used-edge set with the edges already bound by the
                // fixed prefix of this chain, so relationship-uniqueness holds across the
                // fixed→var-length boundary (a `*` segment can't re-walk an earlier hop's
                // edge). Only consulted in Trail mode; varlen only removes edges it itself
                // inserts, so these seeds persist for the whole segment.
                let mut used: HashSet<u64> = walk.iter().map(|h| h.edge).collect();
                // `visited` (node-uniqueness for ACYCLIC/SIMPLE) is seeded with the
                // walk's start node so a hop back to it is detected — rejected by
                // ACYCLIC, allowed once as the closing endpoint by SIMPLE.
                let mut visited = HashSet::new();
                if matches!(mode, WalkMode::Acyclic | WalkMode::Simple) {
                    visited.insert(cur);
                }
                let mut path = Vec::new();
                self.varlen(
                    cur,
                    cur,
                    rel,
                    (min, max),
                    mode,
                    &mut path,
                    &mut used,
                    &mut visited,
                    &mut paths,
                    binding,
                )?;
                for (hops, endnode) in paths {
                    if cap.is_some_and(|c| out.len() >= c) {
                        break;
                    }
                    if !self.node_ok(endnode, next, &Scope::Map(binding), &[])? {
                        continue;
                    }
                    if let Some(v) = &next.var {
                        if let Some(existing) = binding.get(v) {
                            if existing.loose_eq(&Val::Node(endnode)) != Some(true) {
                                continue;
                            }
                        }
                    }
                    let prev_rel = rel.var.as_ref().map(|v| {
                        let rels = Val::List(hops.iter().map(Hop::as_rel).collect());
                        (v.clone(), binding.insert(v.clone(), rels))
                    });
                    let prev_next = next
                        .var
                        .as_ref()
                        .map(|v| (v.clone(), binding.insert(v.clone(), Val::Node(endnode))));
                    let n = hops.len();
                    walk.extend(hops);
                    self.expand_chain(pattern, i + 1, endnode, binding, start, walk, out, cap)?;
                    walk.truncate(walk.len() - n);
                    if let Some((v, prev)) = prev_next {
                        restore_binding(binding, v, prev);
                    }
                    if let Some((v, prev)) = prev_rel {
                        restore_binding(binding, v, prev);
                    }
                }
            }
        }
        Ok(())
    }

    /// Depth-first variable-length expansion, emitting `(path_edges, end_node)` for
    /// every path whose length is in `[min, max]`. `mode` (the GQL path restrictor,
    /// `WalkMode::Trail` by default) governs node/edge reuse within the walk:
    /// - `Walk` — no restriction (repeated nodes and edges allowed). Bounded only by
    ///   `max` (`MAX_VARLEN_HOPS` for an open `*`), the intermediate budget and the
    ///   deadline, since a cycle would otherwise expand without limit.
    /// - `Trail` — no repeated edge (the historical default for `*`); tracked in
    ///   `used`.
    /// - `Acyclic` — no repeated node at all (endpoints included); tracked in
    ///   `visited`, which the caller seeds with the start node.
    /// - `Simple` — no repeated node *except* the two endpoints may coincide (a
    ///   single closed cycle); a hop back to the start node is emitted but not
    ///   extended, so the start can never become an interior repeat.
    ///
    /// Node-uniqueness implies edge-uniqueness, so `Acyclic`/`Simple` need only the
    /// `visited` set and leave `used` untouched; `Trail` uses only `used`. This keeps
    /// each mode's per-hop work minimal and the `Trail`/default path byte-for-byte as
    /// before.
    #[allow(clippy::too_many_arguments)] // recursive DFS: scratch buffers + scope
    pub(crate) fn varlen(
        &self,
        start: u64,
        node: u64,
        rel: &RelPat,
        bounds: (u32, u32),
        mode: WalkMode,
        path: &mut Vec<Hop>,
        used: &mut HashSet<u64>,
        visited: &mut HashSet<u64>,
        out: &mut Vec<(Vec<Hop>, u64)>,
        binding: &HashMap<String, Val>,
    ) -> Result<()> {
        let (min, max) = bounds;
        if path.len() as u32 >= min {
            // Each emission clones the hop vector, so charge by path length: on a
            // dense graph the depth cap alone still permits an enormous result set.
            self.charge(path.len() as u64 + 1)?;
            out.push((path.clone(), node));
        }
        if path.len() as u32 >= max {
            return Ok(());
        }
        self.check_deadline()?;
        let track_edges = matches!(mode, WalkMode::Trail);
        let track_nodes = matches!(mode, WalkMode::Acyclic | WalkMode::Simple);
        for hop in self.expand_one_hop(node, rel, binding)? {
            let edge = hop.edge;
            let nb = hop.neighbour;
            // SIMPLE alone permits the one repeat that closes the walk at its start;
            // it is emitted but never extended (extending would repeat the start as
            // an interior node).
            let mut close_only = false;
            match mode {
                WalkMode::Walk => {}
                WalkMode::Trail => {
                    if used.contains(&edge) {
                        continue;
                    }
                }
                WalkMode::Acyclic => {
                    if visited.contains(&nb) {
                        continue;
                    }
                }
                WalkMode::Simple => {
                    if visited.contains(&nb) {
                        if nb != start {
                            continue;
                        }
                        close_only = true;
                    }
                }
            }
            if track_edges {
                used.insert(edge);
            }
            // `insert` returns false (so `inserted` stays false) when the node is
            // already present — e.g. the SIMPLE close-the-cycle hop back to `start`,
            // which the caller pre-seeded — so we never wrongly remove it on unwind.
            let inserted = track_nodes && visited.insert(nb);
            path.push(hop);
            if close_only {
                if path.len() as u32 >= min {
                    self.charge(path.len() as u64 + 1)?;
                    out.push((path.clone(), nb));
                }
            } else {
                self.varlen(
                    start, nb, rel, bounds, mode, path, used, visited, out, binding,
                )?;
            }
            path.pop();
            if track_edges {
                used.remove(&edge);
            }
            if inserted {
                visited.remove(&nb);
            }
        }
        Ok(())
    }

    /// One traversal step from `node`: edges matching the pattern's direction,
    /// type alternation and relationship property predicates, each resolved to a
    /// [`Hop`] (edge, neighbour, type, and stored src→dst endpoints).
    ///
    /// Charges the produced hops via [`charge_walk`](Self::charge_walk) — the retained
    /// `maxIntermediate` budget in row-building mode, the transient `maxScan` budget in
    /// count-pushdown mode where the adjacency Vec is read-then-discarded (root cause 2b):
    /// expanding a hub reads its whole adjacency and builds one `Hop` per matching
    /// edge — a `Vec<Hop>` that, summed over a depth-first chain walk, is the bulk
    /// of an expansion-heavy query's transient allocation. Without this charge the
    /// terminal `charge(1)` per *completed* row only trips once millions of heavy
    /// binding rows have already materialised (the 2b OOM); charging per produced
    /// hop trips a hub expansion immediately, before those rows accumulate. The
    /// charge is cumulative (never refunded within the query), so a fan-out that
    /// re-expands the same hub at every branch is bounded by total work, not peak.
    ///
    /// Kept on the budgeted traversal wrapper rather than [`Self::expand_with_dir`]
    /// itself: the latter is also the reader for `shortestPath()` reconstruction and
    /// its sequential fallback, which are bounded by the dedicated
    /// `maxShortestPathExplore` cap and must stay independent of `maxIntermediate`.
    pub(crate) fn expand_one_hop(
        &self,
        node: u64,
        rel: &RelPat,
        binding: &HashMap<String, Val>,
    ) -> Result<Vec<Hop>> {
        let hops = self.expand_with_dir(node, rel, rel.dir, binding)?;
        self.charge_walk(hops.len() as u64)?;
        Ok(hops)
    }

    /// As [`Self::expand_one_hop`] but with an explicit traversal `dir`, overriding
    /// `rel.dir`. The bidirectional `shortestPath()` search uses this to walk the
    /// *reverse* of the pattern direction outward from `dst` (so an `(a)-[:T]->(b)`
    /// search expands `dst` over incoming `:T` edges). Type alternation and the
    /// relationship property predicate are still taken from `rel`.
    pub(crate) fn expand_with_dir(
        &self,
        node: u64,
        rel: &RelPat,
        dir: Direction,
        binding: &HashMap<String, Val>,
    ) -> Result<Vec<Hop>> {
        // Resolve the relationship-type constraint once, before the per-edge loop.
        // The overwhelmingly common shapes — untyped, a single `:T`, or a `:T1|T2`
        // alternation — collapse to a flat reltype-id set so the hot loop stays a
        // plain `ids.contains` integer test, exactly as before GQL. Only a genuine
        // boolean type expression (`&`/`!`) falls to per-edge evaluation.
        let type_filter = resolve_type_filter(self.gen, rel);
        // (adjacency list, `incoming`) — for an incoming edge the stored direction
        // is neighbour→node, so start/end are swapped relative to an outgoing one.
        let mut sources: Vec<(Vec<topology::Adj>, bool)> = Vec::new();
        match dir {
            Direction::Outgoing => sources.push((self.outgoing(node)?, false)),
            Direction::Incoming => sources.push((self.incoming(node)?, true)),
            Direction::Undirected => {
                sources.push((self.outgoing(node)?, false));
                sources.push((self.incoming(node)?, true));
            }
        }
        let mut out = Vec::new();
        for (adjs, incoming) in sources {
            for a in adjs {
                match &type_filter {
                    None => {}
                    Some(TypeFilter::AnyOf(ids)) => {
                        if !ids.contains(&a.reltype) {
                            continue;
                        }
                    }
                    // A relationship carries exactly one type, so evaluate the
                    // expression over the singleton present-set {this edge's type}.
                    Some(TypeFilter::Expr(e))
                        if !e.eval(&|name| self.gen.reltype_id(name) == Some(a.reltype)) =>
                    {
                        continue;
                    }
                    Some(TypeFilter::Expr(_)) => {}
                }
                if !self.rel_ok(a.edge.0, rel, binding)? {
                    continue;
                }
                let (start, end) = if incoming {
                    (a.neighbour.0, node)
                } else {
                    (node, a.neighbour.0)
                };
                out.push(Hop {
                    edge: a.edge.0,
                    neighbour: a.neighbour.0,
                    reltype: a.reltype,
                    start,
                    end,
                });
            }
        }
        Ok(out)
    }
}
