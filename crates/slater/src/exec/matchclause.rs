// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for MATCH application, shortest-path and UNWIND.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Run a single query part starting from `seed` instead of the empty singleton.
    /// A top-level query seeds the singleton; a `CALL { … }` subquery seeds the
    /// imported outer variables (one row) so the inner clauses can reference them.
    pub(crate) fn run_single_seeded(&self, sq: &SingleQuery, seed: Table) -> Result<QueryResult> {
        // A pushable `RETURN … LIMIT n` caps only the LAST reading clause feeding
        // the final 1:1 projection — earlier clauses may be filtered or expanded
        // downstream, so capping them could under-produce.
        let cap = self.projection_row_cap(&sq.ret.body, sq.ret.distinct)?;
        let last = sq.reading.len();
        let mut table = seed;
        for (i, clause) in sq.reading.iter().enumerate() {
            let clause_cap = if i + 1 == last { cap } else { None };
            match clause {
                Clause::Match(m) => table = self.apply_match(table, m, clause_cap)?,
                Clause::With(w) => {
                    table = self.project(table, &w.body, w.distinct, w.where_.as_ref())?
                }
                Clause::VectorCall(vc) => table = self.apply_vector_call(table, vc)?,
                Clause::Call(cc) => table = self.apply_call(table, cc)?,
                Clause::CallSubquery(cs) => table = self.apply_call_subquery(table, cs)?,
                Clause::Unwind(uc) => table = self.apply_unwind(table, uc)?,
            }
        }
        let table = self.project(table, &sq.ret.body, sq.ret.distinct, None)?;
        Ok(QueryResult {
            columns: table.cols,
            rows: table.rows,
        })
    }

    // ── MATCH ────────────────────────────────────────────────────────────

    pub(crate) fn apply_match(
        &self,
        table: Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Table> {
        // PR 3: a shortest-path selector (`ANY SHORTEST` / `ALL SHORTEST` /
        // `SHORTEST k`) drives a dedicated search between the pattern's endpoints
        // rather than the ordinary matcher, so route it out first. A selector must be
        // the sole pattern in its clause (comma-joined conjunctions alongside a
        // selector are not yet supported).
        if m.patterns.iter().any(|p| p.selector.is_some()) {
            if m.patterns.len() != 1 {
                bail!(
                    "a path selector (ANY/ALL SHORTEST or SHORTEST k) must be the only \
                     pattern in its MATCH clause"
                );
            }
            return self.apply_match_selected(table, m, cap);
        }
        // PR 2: a path restrictor is honoured only where `varlen` owns the
        // uniqueness scope — a variable-length relationship. Reject it on any other
        // pattern (a node-only or fixed-hop chain) rather than silently ignoring it,
        // so the user gets a clear message instead of unrestricted results. A
        // restrictor over a quantified group is already rejected at lowering.
        for p in &m.patterns {
            if p.restrictor.is_some() && !p.rels.iter().any(|(r, _)| r.var_length.is_some()) {
                bail!(
                    "a path restrictor (WALK/TRAIL/ACYCLIC/SIMPLE) currently requires a \
                     variable-length relationship, e.g. MATCH TRAIL (a)-[:R*]->(b)"
                );
            }
        }
        // Stage 5: a single non-optional node-only pattern (no relationships, no
        // path variable, fresh-scan anchor) streams candidates straight into rows,
        // skipping the per-row `HashMap` binding the general matcher builds (root
        // cause 4).
        if let Some(t) = self.try_stream_match(&table, m, cap)? {
            return Ok(t);
        }
        // GQL quantified path patterns (`((…)){m,n}`) take a separate path that
        // desugars each group into the union of its fixed-length expansions. The
        // common (quantifier-free) case stays on the hot path below untouched.
        if m.patterns.iter().any(|p| p.segments.is_some()) {
            return self.apply_match_quantified(table, m, cap);
        }
        // Variables this clause newly introduces, appended to the scope in order.
        let mut new_vars: Vec<String> = Vec::new();
        for p in &m.patterns {
            collect_pattern_vars(p, &table.cols, &mut new_vars);
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            // Stage 6: stop once a pushed `LIMIT` is satisfied — the cumulative cap
            // across all seed rows. The per-seed match is also capped at the rows
            // still needed, so a single seed expanding millions of paths halts early.
            if cap.is_some_and(|c| out_rows.len() >= c) {
                break;
            }
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }
            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            let remaining = cap.map(|c| c.saturating_sub(out_rows.len()));
            self.match_patterns(
                &m.patterns,
                0,
                seed,
                m.where_.as_ref(),
                &mut matches,
                remaining,
            )?;

            if matches.is_empty() && m.optional {
                let mut r = row.clone();
                r.extend(std::iter::repeat_n(Val::Null, new_vars.len()));
                out_rows.push(r);
            } else {
                for b in matches {
                    let mut r = row.clone();
                    for v in &new_vars {
                        r.push(b.get(v).cloned().unwrap_or(Val::Null));
                    }
                    out_rows.push(r);
                }
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// `MATCH` containing one or more GQL quantified path patterns
    /// (`((…)){m,n}`). Each source pattern is desugared into the union of its
    /// fixed-length expansions (`expand_quantified_pattern`); the cartesian product
    /// of the per-pattern alternatives gives the conjunctive pattern-lists to run.
    /// Every alternative introduces the same named variables (boundary nodes only —
    /// group-internal nodes/relationships are anonymised), so the output column set
    /// is well defined. Each expansion is an ordinary (`segments: None`) pattern, so
    /// it reuses the full matcher, including edge-uniqueness, `node_ok`, the
    /// intermediate budget, and the deadline.
    ///
    /// Semantics: as with Cypher variable-length, one row is emitted per matching
    /// path, so two repetition counts that bind the same boundary nodes produce two
    /// rows (add `DISTINCT` to collapse them) — exactly what `-[*1..2]-` does.
    pub(crate) fn apply_match_quantified(
        &self,
        table: Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Table> {
        let alts: Vec<Vec<Pattern>> = m
            .patterns
            .iter()
            .map(expand_quantified_pattern)
            .collect::<Result<_>>()?;
        let combos = cartesian_patterns(&alts);
        debug_assert!(
            !combos.is_empty(),
            "every quantified group has ≥1 expansion"
        );

        // New variables are identical across combos by construction; derive from the
        // first so the column layout matches every expansion.
        let mut new_vars: Vec<String> = Vec::new();
        for p in &combos[0] {
            collect_pattern_vars(p, &table.cols, &mut new_vars);
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            if cap.is_some_and(|c| out_rows.len() >= c) {
                break;
            }
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }
            // Accumulate all expansions' matches for this seed row before emitting,
            // so OPTIONAL's "no match" test sees every alternative.
            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            let remaining = cap.map(|c| c.saturating_sub(out_rows.len()));
            for combo in &combos {
                if remaining.is_some_and(|r| matches.len() >= r) {
                    break;
                }
                self.match_patterns(
                    combo,
                    0,
                    seed.clone(),
                    m.where_.as_ref(),
                    &mut matches,
                    remaining,
                )?;
            }

            if matches.is_empty() && m.optional {
                let mut r = row.clone();
                r.extend(std::iter::repeat_n(Val::Null, new_vars.len()));
                out_rows.push(r);
            } else {
                for b in matches {
                    if cap.is_some_and(|c| out_rows.len() >= c) {
                        break;
                    }
                    let mut r = row.clone();
                    for v in &new_vars {
                        r.push(b.get(v).cloned().unwrap_or(Val::Null));
                    }
                    out_rows.push(r);
                }
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// `MATCH` carrying a GQL shortest-path selector (`ANY SHORTEST` / `ALL SHORTEST`
    /// / `SHORTEST k`). The pattern is a single relationship between two endpoints;
    /// for every endpoint pair (each side either already bound, or scanned and
    /// filtered by its node pattern) the selector picks shortest connecting paths via
    /// the shared BFS core [`select_paths`] — the same core `shortestPath()` uses.
    /// Each chosen path becomes one output row binding the endpoints, the (list-
    /// valued) relationship variable and any path variable; the clause `WHERE` is
    /// applied per row, exactly as the ordinary matcher does.
    ///
    /// Scope (PR 3): a selector requires a single-relationship pattern (like
    /// `shortestPath()`), carries no relationship property filter, and cannot yet be
    /// combined with a path restrictor — those are rejected with a clear message. A
    /// selector over a quantified group is already rejected at lowering.
    pub(crate) fn apply_match_selected(
        &self,
        table: Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Table> {
        let p = &m.patterns[0];
        let selector = p.selector.expect("routed here only for a selected pattern");
        if p.restrictor.is_some() {
            bail!(
                "combining a path selector with a path restrictor \
                 (WALK/TRAIL/ACYCLIC/SIMPLE) is not yet supported"
            );
        }
        if p.rels.len() != 1 {
            bail!(
                "a path selector (ANY/ALL SHORTEST or SHORTEST k) currently requires a \
                 single relationship, e.g. MATCH ANY SHORTEST (a)-[:R*]->(b)"
            );
        }
        let (rel, end) = &p.rels[0];
        if !rel.props.is_empty() {
            bail!("filters on relationships under a path selector are not supported");
        }
        let (min, max) = match &rel.var_length {
            Some(vl) => varlen_bounds(vl),
            None => (1, 1),
        };

        let mut new_vars: Vec<String> = Vec::new();
        collect_pattern_vars(p, &table.cols, &mut new_vars);
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            if cap.is_some_and(|c| out_rows.len() >= c) {
                break;
            }
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }

            // Endpoint candidates: a bound endpoint is its single node; a free one is
            // scanned and filtered by its node pattern's labels/inline props.
            let srcs = self.endpoint_candidates(&p.start, &seed, m.where_.as_ref())?;
            let dsts = self.endpoint_candidates(end, &seed, m.where_.as_ref())?;

            // Bound the |srcs|×|dsts| search fan-out: with two free endpoints this launches a
            // separate shortest-path search for every (src, dst) pair — quadratic in the scanned
            // id space. Charge the product up front. It is self-scaling: a bound endpoint is a
            // single candidate, so this only bites when *both* endpoints are free and large (the
            // pathological case), and it trips the standard `maxIntermediate` budget before the
            // searches run rather than after they have burned the graph.
            self.charge(srcs.len().saturating_mul(dsts.len()) as u64)?;

            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            for &src in &srcs {
                for &dst in &dsts {
                    for hops in self.select_paths(src, dst, rel, (min, max), selector)? {
                        let mut b = seed.clone();
                        if let Some(v) = &p.start.var {
                            b.insert(v.clone(), Val::Node(src));
                        }
                        // A shared endpoint variable (e.g. `(a)-[*]->(a)`) must agree:
                        // skip the pair when the end node would contradict a binding
                        // the start (or seed) already fixed.
                        if let Some(v) = &end.var {
                            if let Some(existing) = b.get(v) {
                                if existing.loose_eq(&Val::Node(dst)) != Some(true) {
                                    continue;
                                }
                            } else {
                                b.insert(v.clone(), Val::Node(dst));
                            }
                        }
                        if let Some(v) = &rel.var {
                            let rels = Val::List(hops.iter().map(Hop::as_rel).collect());
                            b.insert(v.clone(), rels);
                        }
                        if let Some(pv) = &p.path_var {
                            b.insert(pv.clone(), make_path(src, &hops));
                        }
                        if let Some(w) = m.where_.as_ref() {
                            if !truthy(&self.eval(w, &Scope::Map(&b), None)?) {
                                continue;
                            }
                        }
                        matches.push(b);
                    }
                }
            }

            if matches.is_empty() && m.optional {
                let mut r = row.clone();
                r.extend(std::iter::repeat_n(Val::Null, new_vars.len()));
                out_rows.push(r);
            } else {
                for b in matches {
                    if cap.is_some_and(|c| out_rows.len() >= c) {
                        break;
                    }
                    let mut r = row.clone();
                    for v in &new_vars {
                        r.push(b.get(v).cloned().unwrap_or(Val::Null));
                    }
                    out_rows.push(r);
                }
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// Candidate node ids for one endpoint of a selected pattern. A variable already
    /// bound to a node (by the seed/an earlier clause) is that single node; bound to
    /// a non-node it cannot match (empty). A free endpoint is scanned with the usual
    /// planner strategy and filtered by `node_ok` (its labels + inline props), so an
    /// endpoint like `(b:Person)` only contributes `:Person` nodes.
    pub(crate) fn endpoint_candidates(
        &self,
        node: &NodePat,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
    ) -> Result<Vec<u64>> {
        match node.var.as_deref().and_then(|v| binding.get(v)) {
            Some(Val::Node(id)) => Ok(vec![*id]),
            Some(_) => Ok(Vec::new()),
            None => {
                let bound = bound_scalars(binding);
                let scan = choose_node_scan(self.gen, node, where_, &self.plan_params, &bound);
                let guaranteed = self.scan_guaranteed_labels(&scan);
                // Streamed: only the survivors are retained, never the candidate set as
                // well (an endpoint of a quantified pattern can be a full-width scan).
                let mut out = Vec::new();
                let mut stream = self.candidate_stream(&scan)?;
                while let Some(batch) = self.next_candidates(&mut stream)? {
                    for &c in batch {
                        if self.node_ok(c, node, &Scope::Map(binding), &guaranteed)? {
                            out.push(c);
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Shared shortest-path BFS core driving both `shortestPath()` and the GQL path
    /// selectors. Between two concrete nodes `src`/`dst` it returns the chosen paths
    /// as hop-lists in walk (start→end) order:
    /// - `AnyShortest` → at most one shortest path;
    /// - `AllShortest` → every path of the single minimum length;
    /// - `ShortestK(k)` → up to `k` paths in non-decreasing length order.
    ///
    /// `AnyShortest` (the `shortestPath()` case) needs just one path, so it runs a
    /// single global-`visited` BFS with a back-pointer map ([`Self::any_shortest_path`]):
    /// each node is enqueued at most once (frontier ≤ |V|, work `O(V+E)`), BFS first
    /// reaches every node along a shortest path, and the reconstructed walk is
    /// automatically simple.
    ///
    /// `AllShortest`/`ShortestK` can have exponentially many shortest paths, so they
    /// keep the loopless simple-path search below: each frontier entry carries its own
    /// cloned `visited` set, so a node reachable by many prefixes is re-enqueued once
    /// per prefix. On a hub-dense small-world graph that frontier explodes, so the
    /// per-layer `maxIntermediate` charge is its backstop (it rejects the blow-up
    /// instead of OOMing). Paths are loopless (no node repeats), bounding the walk on a
    /// cyclic graph; every entry in a BFS layer has the same hop count, so paths
    /// surface in non-decreasing length order — the property those selectors rely on.
    /// `min`/`max` are the relationship's length bounds (a fixed hop is `(1, 1)`);
    /// `min == 0` with coincident endpoints admits the empty path.
    pub(crate) fn select_paths(
        &self,
        src: u64,
        dst: u64,
        rel: &RelPat,
        bounds: (u32, u32),
        selector: PathSelector,
    ) -> Result<Vec<Vec<Hop>>> {
        let (min, max) = bounds;
        let empty = HashMap::new();
        if matches!(selector, PathSelector::AnyShortest) {
            return self.any_shortest_path(src, dst, rel, bounds);
        }
        let want = match selector {
            PathSelector::AnyShortest => 1,
            PathSelector::ShortestK(k) => k as usize,
            PathSelector::AllShortest => usize::MAX,
        };
        let mut results: Vec<Vec<Hop>> = Vec::new();

        // min == 0 admits the empty (single-node) path when the endpoints coincide.
        if min == 0 && src == dst {
            results.push(Vec::new());
            if results.len() >= want {
                return Ok(results);
            }
        }
        if max == 0 {
            return Ok(results);
        }

        // Each frontier entry carries its own loopless `visited` set so sibling
        // branches stay simple independently. (node, path so far, visited nodes).
        let mut frontier: Vec<(u64, Vec<Hop>, HashSet<u64>)> =
            vec![(src, Vec::new(), HashSet::from([src]))];
        let mut depth = 0u32;
        // `AllShortest`: once `dst` is first reached, its layer is the minimum length;
        // after that layer is fully processed no further shortest path can appear.
        let mut found_min = false;
        while !frontier.is_empty() && depth < max {
            self.check_deadline()?;
            let mut next = Vec::new();
            for (node, path, visited) in &frontier {
                for hop in self.expand_one_hop(*node, rel, &empty)? {
                    let nb = hop.neighbour;
                    if visited.contains(&nb) {
                        continue; // loopless: never revisit a node on this path
                    }
                    if nb == dst {
                        // A connecting path ends here; a loopless path is never
                        // extended past its destination.
                        let len = path.len() as u32 + 1;
                        if len >= min {
                            let mut hops = path.clone();
                            hops.push(hop);
                            self.charge(hops.len() as u64 + 1)?;
                            results.push(hops);
                            found_min = true;
                            if results.len() >= want {
                                return Ok(results);
                            }
                        }
                        continue;
                    }
                    // Charge this live branch *before* cloning its path + visited set.
                    // Each branch carries a cloned `Vec<Hop>` + cloned `HashSet<u64>`, so
                    // on a hub-dense small-world graph a single layer's frontier can
                    // explode to millions of entries. Charging the whole layer only
                    // *after* it is materialised lets that one layer exhaust RSS before
                    // the budget ever trips (it OOM-killed the capped container); charging
                    // per branch trips the standard `maxIntermediate` budget mid-layer,
                    // before the clones accumulate. The charge is **proportional to the
                    // branch's clone size** (`path.len()+1`, mirroring the result charge
                    // above) — a fixed `charge(1)` under-counted a deep branch by a factor
                    // of its depth, so the budget only tripped long after the O(depth)
                    // clones had accumulated. Only emitted results are charged elsewhere.
                    self.charge(path.len() as u64 + 1)?;
                    let mut npath = path.clone();
                    npath.push(hop);
                    let mut nvisited = visited.clone();
                    nvisited.insert(nb);
                    next.push((nb, npath, nvisited));
                }
            }
            // `AllShortest` stops after the first dst-bearing layer; the others stop
            // only on `want`/exhaustion (handled above and by the loop condition).
            if found_min && matches!(selector, PathSelector::AllShortest) {
                return Ok(results);
            }
            frontier = next;
            depth += 1;
        }
        Ok(results)
    }

    /// `ANY SHORTEST` / `shortestPath()`: one shortest path between `src` and `dst`,
    /// via **bidirectional** BFS — a forward search from `src` along the pattern
    /// direction and a backward search from `dst` along the *reverse* direction,
    /// expanding the smaller frontier each step until the two search spheres meet.
    ///
    /// Why bidirectional: on a small-world / scale-free graph the k-hop ball grows
    /// roughly exponentially in k, so a one-sided BFS to depth `max` can touch a large
    /// fraction of a giant component (≈766 M edge reads on full Wikidata → minutes,
    /// I/O-bound). Meeting in the middle replaces one depth-`max` ball with two
    /// depth-`max/2` balls — exponentially less work *and* memory. Each side keeps a
    /// dense bitset `visited` (≈ node_count/8 bytes) plus a `node -> (neighbour, depth)`
    /// map; the discovering `Hop`s are re-derived from the CSR during reconstruction,
    /// so the resident structures stay small. The deadline is checked *within* a level
    /// (every few thousand expansions) so a runaway search aborts at `timeoutMs` rather
    /// than overrunning between levels. The optional, dedicated `maxShortestPathExplore`
    /// cap (0 = unlimited) bounds the total nodes either search may hold, independent of
    /// the shared `maxIntermediate` budget — preserving the always-succeeds guarantee
    /// by default.
    ///
    /// `max` caps the *total* path length; `min` filters the result. `min == 0` with
    /// coincident endpoints admits the empty path. (For `shortestPath()` `min ∈ {0,1}`,
    /// so the discovered shortest distance always meets it whenever a path exists.)
    pub(crate) fn any_shortest_path(
        &self,
        src: u64,
        dst: u64,
        rel: &RelPat,
        bounds: (u32, u32),
    ) -> Result<Vec<Vec<Hop>>> {
        let (min, max) = bounds;
        let empty = HashMap::new();

        // min == 0 admits the empty (single-node) path when the endpoints coincide.
        if min == 0 && src == dst {
            return Ok(vec![Vec::new()]);
        }
        if max == 0 {
            return Ok(Vec::new());
        }

        let node_count = self.gen.node_count();
        let words = node_count.div_ceil(64) as usize;
        let (mut fvis, mut bvis) = (vec![0u64; words], vec![0u64; words]);
        let bit = |v: &[u64], id: u64| (v[(id >> 6) as usize] >> (id & 63)) & 1 != 0;
        let set = |v: &mut [u64], id: u64| v[(id >> 6) as usize] |= 1u64 << (id & 63);
        set(&mut fvis, src);
        set(&mut bvis, dst);

        let cap = self.max_shortest_path_explore;
        let mut discovered: u64 = 2; // the two endpoints are already held resident
        if cap != 0 && discovered > cap {
            return Err(ExecLimit::ShortestPathCap(cap).into());
        }

        // `node -> (neighbour toward the seed, depth from the seed)`.
        let mut fpar: HashMap<u64, (u64, u32)> = HashMap::new();
        let mut bpar: HashMap<u64, (u64, u32)> = HashMap::new();
        let (mut ffront, mut bfront) = (vec![src], vec![dst]);
        let (mut fdepth, mut bdepth) = (0u32, 0u32);
        let fdir = rel.dir;
        let bdir = match rel.dir {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Undirected => Direction::Undirected,
        };
        // Parallel frontier expansion is sound only for a property-free pattern whose
        // type constraint is a flat reltype-id set (or absent): the off-thread worker
        // reads adjacency but must not evaluate a rel predicate (that would touch the
        // executor's interior-mutable state). Anything richer expands sequentially.
        let type_ids: Option<Vec<u32>> = rel.type_expr.as_ref().and_then(|e| {
            e.positive_atoms().map(|names| {
                names
                    .iter()
                    .filter_map(|t| self.gen.reltype_id(t))
                    .collect()
            })
        });
        let fast = rel.props.is_empty() && (rel.type_expr.is_none() || type_ids.is_some());
        let mut best: Option<(u32, u64)> = None; // (total length, meeting node)
        let mut since_check = 0u64;
        const SP_PAR_MIN_FRONTIER: usize = 64; // below this, the pool overhead isn't worth it

        loop {
            self.check_deadline()?;
            let combined = fdepth + bdepth;
            let bound = best.map(|(b, _)| b.min(max)).unwrap_or(max);
            // No future meeting can be shorter than `combined + 1`, so once the two
            // radii sum to the best-so-far (or `max`) we are done.
            if combined >= bound || ffront.is_empty() || bfront.is_empty() {
                break;
            }

            // Expand whichever frontier is smaller (the bidirectional speed-up).
            let forward = ffront.len() <= bfront.len();
            let (front, dir, depth) = if forward {
                (&ffront, fdir, fdepth + 1)
            } else {
                (&bfront, bdir, bdepth + 1)
            };

            // Gather the level's neighbours per frontier node. The I/O-bound adjacency
            // reads (CSR block fetch + zstd decompress, released from the cache mutex)
            // overlap across the pool; all `visited`/`parent`/meeting mutation happens
            // single-threaded in the merge below.
            let expansions: Vec<(u64, Vec<u64>)> = if fast {
                let tids = type_ids.as_deref();
                let (gen, cache) = (self.gen, self.cache);
                par_gather(
                    self.fanout_pool.as_deref(),
                    front,
                    SP_PAR_MIN_FRONTIER,
                    |&node| neighbours_par(gen, cache, node, dir, tids).map(|nbs| (node, nbs)),
                )?
            } else {
                let mut v = Vec::with_capacity(front.len());
                for &node in front {
                    let nbs = self
                        .expand_with_dir(node, rel, dir, &empty)?
                        .into_iter()
                        .map(|h| h.neighbour)
                        .collect();
                    v.push((node, nbs));
                }
                v
            };

            let mut next = Vec::new();
            for (node, nbs) in expansions {
                for nb in nbs {
                    let (mine, theirs) = if forward {
                        (&mut fvis, &bvis)
                    } else {
                        (&mut bvis, &fvis)
                    };
                    if bit(mine, nb) {
                        continue; // already on a ≤-length shortest path this side
                    }
                    set(mine, nb);
                    discovered += 1;
                    if cap != 0 && discovered > cap {
                        return Err(ExecLimit::ShortestPathCap(cap).into());
                    }
                    if forward {
                        fpar.insert(nb, (node, depth));
                    } else {
                        bpar.insert(nb, (node, depth));
                    }
                    if bit(theirs, nb) {
                        // The other search already reached `nb`: its depth there is the
                        // seed (0) or the recorded value.
                        let other = if forward { &bpar } else { &fpar };
                        let other_seed = if forward { dst } else { src };
                        let od = if nb == other_seed {
                            0
                        } else {
                            other.get(&nb).map(|&(_, d)| d).unwrap_or(0)
                        };
                        let total = depth + od;
                        if total >= min && best.map(|(b, _)| total < b).unwrap_or(true) {
                            best = Some((total, nb));
                        }
                    }
                    next.push(nb);
                    since_check += 1;
                    if since_check >= 4096 {
                        self.check_deadline()?;
                        since_check = 0;
                    }
                }
            }
            if forward {
                ffront = next;
                fdepth = depth;
            } else {
                bfront = next;
                bdepth = depth;
            }
        }

        match best {
            Some((total, meet)) if total >= min && total <= max => {
                let nodes = bidir_node_path(src, dst, meet, &fpar, &bpar);
                Ok(vec![self.reconstruct_from_node_path(&nodes, rel)?])
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Rebuild the hop sequence for a node path (consecutive ids in walk order),
    /// re-deriving each [`Hop`] from the CSR — the bidirectional search stored only
    /// neighbour ids to keep its working set small. For each step `u -> v` we take the
    /// first pattern-typed edge from `u` that lands on `v`; any such edge is a valid
    /// shortest-path edge (parallel multi-edges are interchangeable for `AnyShortest`).
    pub(crate) fn reconstruct_from_node_path(
        &self,
        nodes: &[u64],
        rel: &RelPat,
    ) -> Result<Vec<Hop>> {
        let empty = HashMap::new();
        let mut hops = Vec::with_capacity(nodes.len().saturating_sub(1));
        for w in nodes.windows(2) {
            let (u, v) = (w[0], w[1]);
            let hop = self
                .expand_with_dir(u, rel, rel.dir, &empty)?
                .into_iter()
                .find(|h| h.neighbour == v)
                .expect("path edge re-derivable from the CSR");
            hops.push(hop);
        }
        Ok(hops)
    }

    /// Stream a single node-only `MATCH` (one pattern, no relationships, no path
    /// variable, anchor not already bound) directly into output rows, returning
    /// the new table or `None` when the pattern needs the general matcher.
    ///
    /// The general path materialises a `Vec<HashMap<String, Val>>` — one cloned
    /// binding map per matched row (root cause 4). For a bare label/index scan the
    /// only new binding is the anchor node, so we append `Val::Node(id)` to a clone
    /// of the input row and skip the map entirely. The anchor scan is chosen once
    /// (parameter/`WHERE`-aware, like the general path), `node_ok` enforces the
    /// pattern's labels/inline props, and the clause `WHERE` is re-evaluated per
    /// emitted row against the full row scope — identical semantics to
    /// `match_patterns`, including row order and the per-row intermediate charge.
    pub(crate) fn try_stream_match(
        &self,
        table: &Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Option<Table>> {
        if m.optional || m.patterns.len() != 1 {
            return Ok(None);
        }
        let p = &m.patterns[0];
        if !p.rels.is_empty() || p.path_var.is_some() || p.segments.is_some() {
            return Ok(None);
        }
        let start = &p.start;
        // An already-bound anchor is a single concrete node, handled by the general
        // matcher's bound-anchor branch; only a fresh scan streams here.
        if let Some(v) = &start.var {
            if table.cols.contains(v) {
                return Ok(None);
            }
        }

        // A *correlated* anchor keys its index off a column already in `table`
        // (e.g. `UNWIND $ids AS w MATCH (n:L {p: w})`, or `WHERE n.p = w`): the seek
        // depends on the row, so the scan must move inside the loop. When the anchor
        // is uncorrelated we plan once and reuse it for every row — today's fast path.
        let correlated = anchor_correlated(start, m.where_.as_ref(), &table.cols);
        let hoisted = if correlated {
            None
        } else {
            let scan = choose_node_scan(
                self.gen,
                start,
                m.where_.as_ref(),
                &self.plan_params,
                &HashMap::new(),
            );
            let guaranteed = self.scan_guaranteed_labels(&scan);
            Some((guaranteed, scan))
        };
        // A *multi-row* input against an uncorrelated anchor is a cartesian product: every
        // input row revisits every candidate, so derive the ids once and replay them rather
        // than re-running the sweep per row (which would re-read the label column once per
        // row). A single input row — the seed, or a `WITH` that collapsed to one — streams,
        // so `LIMIT` short-circuits the scan: the case the bounded-memory invariant turns on.
        let shared: Option<Vec<u64>> = match &hoisted {
            Some((_, scan)) if table.rows.len() > 1 => Some(self.scan_candidates(scan)?),
            _ => None,
        };

        let mut out_cols = table.cols.clone();
        if let Some(v) = &start.var {
            out_cols.push(v.clone());
        }

        let mut out_rows = Vec::new();
        'outer: for in_row in &table.rows {
            self.check_deadline()?;
            // Binding for inline-prop evaluation in `node_ok`, built once per input
            // row (the anchor's own var is intentionally absent, as in the general
            // path). Typically one row — the singleton seed — so one map per query.
            let in_binding: HashMap<String, Val> = table
                .cols
                .iter()
                .cloned()
                .zip(in_row.iter().cloned())
                .collect();
            // The hoisted plan (streamed, or replayed from the shared set), or a per-row
            // index seek keyed by this row's scalars.
            let per_row;
            let (guaranteed, mut stream): (&[u32], CandidateStream) = match (&hoisted, &shared) {
                (Some((g, _)), Some(ids)) => (g, CandidateStream::ready(ids)),
                (Some((g, scan)), None) => (g, self.candidate_stream(scan)?),
                (None, _) => {
                    let bound = bound_scalars(&in_binding);
                    let scan = choose_node_scan(
                        self.gen,
                        start,
                        m.where_.as_ref(),
                        &self.plan_params,
                        &bound,
                    );
                    per_row = self.scan_guaranteed_labels(&scan);
                    let stream = self.candidate_stream(&scan)?;
                    (&per_row, stream)
                }
            };
            while let Some(batch) = self.next_candidates(&mut stream)? {
                for &c in batch {
                    // Stage 6: honour a pushed `LIMIT` (no ORDER BY/aggregation/DISTINCT)
                    // so a bare `MATCH (n:L) … LIMIT k` scans only k matching nodes — and,
                    // now that the scan is a stream, stops producing candidates too.
                    if cap.is_some_and(|cc| out_rows.len() >= cc) {
                        break 'outer;
                    }
                    if !self.node_ok(c, start, &Scope::Map(&in_binding), guaranteed)? {
                        continue;
                    }
                    let mut row = in_row.clone();
                    if start.var.is_some() {
                        row.push(Val::Node(c));
                    }
                    if let Some(w) = m.where_.as_ref() {
                        if !truthy(&self.eval(w, &Scope::Row(&out_cols, &row), None)?) {
                            continue;
                        }
                    }
                    self.charge(1)?;
                    out_rows.push(row);
                }
            }
        }
        Ok(Some(Table {
            cols: out_cols,
            rows: out_rows,
        }))
    }

    // ── UNWIND ───────────────────────────────────────────────────────────────

    /// Multiply each input row by the elements of the list `uc.expr` evaluates to,
    /// binding each element to `uc.var`. Matching FalkorDB's `op_unwind` (`_initList`):
    /// a list expands element-wise, NULL and the empty list emit zero rows, and any
    /// other scalar is wrapped as a single-element list (one row) — a deliberate
    /// FalkorDB divergence from Neo4j (which errors on `UNWIND 5`).
    pub(crate) fn apply_unwind(&self, table: Table, uc: &UnwindClause) -> Result<Table> {
        let mut out_cols = table.cols.clone();
        // The alias is a fresh binding appended after the input columns. (A name
        // clash with an existing column would shadow it on read; the eu-ai-act
        // service never re-uses an in-scope name here.)
        out_cols.push(uc.var.clone());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let items = match self.eval(&uc.expr, &scope, None)? {
                Val::List(xs) => xs,
                Val::Null => continue,  // null → zero rows
                scalar => vec![scalar], // scalar → wrap as [scalar] → one row
            };
            for item in items {
                let mut r = row.clone();
                r.push(item);
                self.charge(r.len() as u64)?;
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    // ── CALL <metadata procedure> (Phase 11) ────────────────────────────────
}
