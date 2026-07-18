// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for the query driver (`run`) and the count/metadata fast paths.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Execute a (possibly `UNION`ed) query. Always refunds this query's
    /// server-wide budget charge before returning, on success or failure.
    pub fn run(&self, q: &Query) -> Result<QueryResult> {
        let r = self.run_inner(q);
        self.release_global();
        r
    }

    pub(crate) fn run_inner(&self, q: &Query) -> Result<QueryResult> {
        self.budget_used.set(0); // per-run budget; engines may be reused
        self.scan_used.set(0);
        self.scanned_ids.set(0);
        self.global_charged.set(0);
        let mut result = self.run_single(&q.head)?;
        for (union_all, part) in &q.tail {
            let next = self.run_single(part)?;
            if next.columns.len() != result.columns.len() {
                bail!("all parts of a UNION must return the same number of columns");
            }
            self.charge(next.rows.len() as u64)?; // UNION cross-branch buildup
            result.rows.extend(next.rows);
            if !union_all {
                self.charge(result.rows.len() as u64)?; // DISTINCT `seen` set
                dedup_rows(&mut result.rows);
            }
        }
        if result.rows.len() > self.max_rows {
            bail!(
                "query produced {} rows, exceeding the limit of {}",
                result.rows.len(),
                self.max_rows
            );
        }
        Ok(result)
    }

    pub(crate) fn run_single(&self, sq: &SingleQuery) -> Result<QueryResult> {
        // The count / whole-graph-metadata fast paths answer from the immutable core's
        // resident marginals and range indexes without materialising rows — but a live
        // delta can change those answers (a tombstone removes a node from a count/label
        // enumeration; a property patch on an indexed key moves it in the index).
        //
        // The bare `count(*)` path is delta-aware and always runs: the delta carries an
        // O(1) born tally per level and a small suppressed-id set, so the merged count is
        // still a metadata read. The rest need a pure core; with any delta present they
        // fall through to full execution, where `scan_candidates` suppresses tombstones
        // and the property overlay corrects patched values. The empty delta is the
        // overwhelming common case, so read-only performance is intact.
        // Stage 3: a bare `MATCH (n[:L][{p: v}]) RETURN count(*)|count(n)` from
        // resident metadata / a single index lookup, skipping materialisation.
        // Only reachable here (top-level / UNION part), where the seed is the
        // empty singleton — a `CALL { … }` subquery seeds outer rows via
        // `run_single_seeded` and never takes this path, so the count is always
        // over the whole match.
        //
        // This one is **delta-aware** (`live_node_count` / `live_label_node_count` net
        // out the delta's born and suppressed rows), so it survives a non-empty delta —
        // without it, a single `MERGE` would turn a whole-graph `count(*)` into a full
        // scan of the core. The inline-property variant still needs a pure core (see
        // the guard in `try_count_fast_path`).
        if let Some((columns, row)) = self.try_count_fast_path(sq)? {
            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }
        // Stage E: the bare whole-graph edge count `MATCH ()-[r]->() RETURN count(*)`,
        // from resident counts rather than an expansion. Delta-aware; it must precede
        // Stage B, which would otherwise walk every edge.
        if let Some((columns, row)) = self.try_edge_count_fast_path(sq)? {
            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }
        // Stage M: a whole-graph label/reltype *metadata* enumeration or grouped count —
        // `MATCH ()-[r]->() RETURN [DISTINCT] type(r) [, count(*)]` and `MATCH (n) RETURN
        // [DISTINCT] labels(n)[0] [, count(*)]` (plus the labelled schema-marginal
        // variants) — answered from resident metadata with zero block reads, instead of
        // materialising every binding. Both are delta-aware: they net the delta's born
        // rows in and its suppressed rows out, and decline the shapes they cannot answer
        // exactly over a delta (the labelled endpoint cube, an undirected hop).
        if let Some(res) = self.try_reltype_meta_fast_path(sq)? {
            return Ok(res);
        }
        if let Some(res) = self.try_label_meta_fast_path(sq)? {
            return Ok(res);
        }
        // The grouped-index fast path walks the base range index / histograms directly, which
        // are not segment-aware, so it is only sound over a singleton set; a stacked set falls
        // through to full execution (segment-aware via the scan / adjacency seams).
        if self.gen.delta().is_empty() && self.gen.core_stack().is_singleton() {
            // Stage 7: `MATCH (n:L) RETURN n.p, count(*)` (group-by an indexed prop)
            // and `RETURN count(DISTINCT n.p)` are answered from the range index over
            // (L, p) — one sequential index walk, no per-node property decode.
            if let Some(res) = self.try_grouped_index_fast_path(sq)? {
                return Ok(res);
            }
            // Stage B: `MATCH (…)-[…]->(…) [WHERE …] RETURN count(*)|count(v)` — a
            // multi-hop count walks but counts during expansion instead of
            // materialising the row set (the fanout RSS peak).
            if let Some(res) = self.try_count_walk_fast_path(sq)? {
                return Ok(res);
            }
        }
        self.run_single_seeded(sq, Table::singleton())
    }

    /// Recognise a single-node `count` aggregate that can be answered without
    /// materialising rows, returning the single result `(columns, row)` or `None`
    /// when any guard fails (the caller then executes normally).
    ///
    /// Guards: exactly one MATCH reading clause, non-OPTIONAL, no WHERE, one
    /// single-node pattern (no rels); the RETURN is non-DISTINCT, no
    /// ORDER BY/SKIP/LIMIT/`*`, and its items are exactly one `count(*)`/`count(n)`
    /// (n the pattern's variable) plus any number of **constant** items
    /// (`$param`/literal — the benchmark appends `… , $k AS k` to bust the result
    /// cache). A constant item is a single grouping key with one group, so the
    /// count is still over the whole match.
    ///
    /// The count itself: no inline props → `node_count()` (0 labels) or
    /// `label_node_count(L)` (1 label); a single indexed-equality inline prop
    /// whose index covers exactly the pattern's label+prop → that index's
    /// `lookup_eq` length. Anything else (multi-label, residual props, non-index
    /// props, a non-constant extra projection) falls back.
    pub(crate) fn try_count_fast_path(
        &self,
        sq: &SingleQuery,
    ) -> Result<Option<(Vec<String>, Vec<Val>)>> {
        // A stacked set answers `count(*)` / `count(n:L)` from the summed segment marginals
        // (`live_node_count` / `live_label_node_count`); decline to full execution when any
        // segment's marginals are not provably exact.
        if !self.gen.core_stack().marginals_exact() {
            return Ok(None);
        }
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if !pat.rels.is_empty() || pat.segments.is_some() {
            return Ok(None); // single-node patterns only
        }
        let node = &pat.start;

        let body = &sq.ret.body;
        if sq.ret.distinct
            || body.star
            || body.items.is_empty()
            || !body.order_by.is_empty()
            || body.skip.is_some()
            || body.limit.is_some()
        {
            return Ok(None);
        }
        // Exactly one item must be the count; every other item must be a constant
        // (a single, constant grouping key → one group).
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_count_of(&it.expr, node.var.as_deref()) {
                if count_idx.is_some() {
                    return Ok(None); // two counts — not our shape
                }
                count_idx = Some(i);
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None); // a non-constant projection ⇒ grouping/other agg
            }
        }
        let Some(count_idx) = count_idx else {
            return Ok(None);
        };

        // Compute the match count. The no-inline-prop shapes read the *live* counts, so
        // they hold over a merged view too (born rows added, suppressed rows netted out).
        let count: i64 = if node.props.is_empty() {
            match &node.label_expr {
                None => self.gen.live_node_count() as i64,
                // A lone positive atom is a single label posting; any boolean /
                // multi-label expression has no single-posting count — fall back.
                Some(e) => match e.as_single_atom() {
                    Some(l) => match self.gen.label_id(l) {
                        // `live_label_node_count` is exact under a label overlay (Stage 5),
                        // so no fall-back-to-scan is needed here.
                        Some(lid) => self.gen.live_label_node_count(lid)? as i64,
                        // A label the core never defined can still have delta-born nodes
                        // (a `MERGE` may introduce a brand-new label), and those have no
                        // core label id to count against — fall back to full execution.
                        None if !self.gen.delta().is_empty() => return Ok(None),
                        None => 0,
                    },
                    None => return Ok(None),
                },
            }
        } else if !self.gen.delta().is_empty() {
            // Inline props over a delta: the index-length shortcut below ignores born
            // rows, moved indexed values and tombstones. It is also cheap to execute
            // normally (an indexed seek, not a scan), so just fall back.
            return Ok(None);
        } else {
            // Inline props: only an exact single indexed-equality is safe (the scan
            // result then needs no residual filtering, so its length is the count).
            let scan = choose_node_scan(self.gen, node, None, &self.plan_params, &HashMap::new());
            let NodeScan::RangeEq { ref index, .. } = scan else {
                return Ok(None);
            };
            let covers = node.props.len() == 1
                && self
                    .gen
                    .manifest()
                    .range_indexes
                    .iter()
                    .find(|ri| &ri.name == index && ri.entity == EntityKind::Node)
                    .is_some_and(|ri| {
                        // The RangeEq scan fully determines membership only when no
                        // label residual remains: either no label constraint, or a
                        // single positive atom that *is* the index's label. A boolean
                        // or multi-label expression would need re-checking, so bail.
                        node.props[0].0 == ri.property
                            && match &node.label_expr {
                                None => true,
                                Some(e) => {
                                    e.as_single_atom().map(String::as_str)
                                        == Some(ri.label_or_type.as_str())
                                }
                            }
                    });
            if !covers {
                return Ok(None);
            }
            self.scan_candidates(&scan)?.len() as i64
        };

        Ok(Some(self.count_row(sq, count_idx, count)?))
    }

    /// Build the single output row of a count fast path: `count` in its column, every
    /// other (constant) projection evaluated against an empty scope.
    pub(crate) fn count_row(
        &self,
        sq: &SingleQuery,
        count_idx: usize,
        count: i64,
    ) -> Result<(Vec<String>, Vec<Val>)> {
        let body = &sq.ret.body;
        let empty: HashMap<String, Val> = HashMap::new();
        let mut columns = Vec::with_capacity(body.items.len());
        let mut row = Vec::with_capacity(body.items.len());
        for (i, it) in body.items.iter().enumerate() {
            columns.push(it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)));
            if i == count_idx {
                row.push(Val::Int(count));
            } else {
                row.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
            }
        }
        Ok((columns, row))
    }

    /// Recognise a whole-graph relationship-type metadata query and answer it from
    /// resident metadata (the per-reltype edge counts / edge-schema marginals),
    /// touching no blocks. Handles the enumeration `MATCH ()-[r]->() RETURN DISTINCT
    /// type(r)` and the grouped count `RETURN type(r), count(*)`, plus the
    /// source/target-labelled marginals `(:A)-[r]->()` / `()-[r]->(:B)` (in either
    /// arrow direction) when the generation carries the schema marginals.
    ///
    /// Declines (→ `None`, the matcher runs) on anything that makes it more than a
    /// whole-graph metadata question: a WHERE, a rel-type filter or rel property, an
    /// endpoint property or boolean/multi-label expr, both endpoints labelled (the
    /// full cube — currently unbuilt), an undirected relationship (the `2·edge −
    /// self_loop` semantics are deferred to a parity-checked follow-up), any extra
    /// non-constant projection, additional pattern segments, or ORDER BY/SKIP/LIMIT.
    pub(crate) fn try_reltype_meta_fast_path(
        &self,
        sq: &SingleQuery,
    ) -> Result<Option<QueryResult>> {
        // ---- pattern shape ----
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if pat.segments.is_some() || pat.selector.is_some() || pat.restrictor.is_some() {
            return Ok(None);
        }
        if pat.rels.len() != 1 {
            return Ok(None); // exactly one relationship (a whole-graph edge scan)
        }
        let (rel, right) = &pat.rels[0];
        let left = &pat.start;
        // The relationship must be an unfiltered single hop, bound to a variable so
        // `type(r)` can reference it.
        if rel.type_expr.is_some() || !rel.props.is_empty() || rel.var_length.is_some() {
            return Ok(None);
        }
        let Some(relvar) = rel.var.as_deref() else {
            return Ok(None);
        };
        // Endpoints carry no properties.
        if !left.props.is_empty() || !right.props.is_empty() {
            return Ok(None);
        }
        // A single node variable reused on both endpoints (`(a)-[r]->(a)`) constrains
        // the edge to a self-loop — that is not a whole-graph metadata question, so
        // decline and let the matcher handle it.
        if let (Some(lv), Some(rv)) = (left.var.as_deref(), right.var.as_deref()) {
            if lv == rv {
                return Ok(None);
            }
        }
        // Each endpoint is bare (no constraint) or a single positive label atom.
        let atom = |n: &NodePat| -> Result<Option<Option<String>>> {
            match &n.label_expr {
                None => Ok(Some(None)),
                Some(e) => match e.as_single_atom() {
                    Some(l) => Ok(Some(Some(l.clone()))),
                    None => Ok(None), // boolean / multi-label ⇒ decline
                },
            }
        };
        let (Some(left_label), Some(right_label)) = (atom(left)?, atom(right)?) else {
            return Ok(None);
        };

        // ---- projection shape ----
        let Some((key_idx, count_idx)) =
            self.classify_meta_projection(sq, |e| is_type_of(e, Some(relvar)), relvar)
        else {
            return Ok(None);
        };

        // ---- resolve the metadata source and compute per-reltype counts ----
        // Each endpoint resolves to: `None` ⇒ bare (no label); `Some(None)` ⇒ labelled
        // but the label is absent from the graph (matches nothing); `Some(Some(id))` ⇒
        // labelled with a known id.
        let left_id = left_label.map(|n| self.gen.label_id(&n));
        let right_id = right_label.map(|n| self.gen.label_id(&n));

        // With a live delta *or* a core segment stack, the base's resident schema marginals
        // no longer describe the graph. The whole-graph `type(r)` shape is still answerable
        // from the summed edge counters (`live_reltype_edge_groups`); the labelled-endpoint
        // cube and the undirected doubling are not, so they decline and the matcher runs.
        if !self.gen.delta().is_empty() || !self.gen.core_stack().is_singleton() {
            if left_id.is_some() || right_id.is_some() || matches!(rel.dir, Direction::Undirected) {
                return Ok(None);
            }
            let Some(live) = self.gen.live_reltype_edge_groups()? else {
                return Ok(None);
            };
            let groups: Vec<(Val, u64)> = live.into_iter().map(|(n, c)| (Val::Str(n), c)).collect();
            return Ok(Some(
                self.build_meta_result(sq, key_idx, count_idx, groups)?,
            ));
        }
        // Edges of type `t` whose source satisfies `src` and target satisfies `tgt`,
        // read from the resident whole-graph counts / schema marginals / cube. `None`
        // ⇒ the required marginal is not present in this generation (⇒ decline).
        let g = |src: Option<Option<u32>>, tgt: Option<Option<u32>>, t: u32| -> Option<u64> {
            Some(match (src, tgt) {
                (None, None) => self.gen.reltype_edge_count(t),
                (Some(None), _) | (_, Some(None)) => 0,
                (Some(Some(a)), None) => self.gen.src_label_reltype_count(a, t)?,
                (None, Some(Some(b))) => self.gen.reltype_tgt_label_count(t, b)?,
                (Some(Some(a)), Some(Some(b))) => self.gen.schema_triple_count(a, t, b)?,
            })
        };
        // Map the pattern's directionality onto the source/target axes. An outgoing
        // arrow binds left→source, right→target; incoming is the mirror. An undirected
        // relationship matches each edge in *both* orientations, so its count is the
        // sum over both axis assignments — which counts a self-loop twice and handles
        // a labelled endpoint "on either end" without any inclusion-exclusion.
        let count_for = |t: u32| -> Option<u64> {
            match rel.dir {
                Direction::Outgoing => g(left_id, right_id, t),
                Direction::Incoming => g(right_id, left_id, t),
                Direction::Undirected => Some(g(left_id, right_id, t)? + g(right_id, left_id, t)?),
            }
        };

        let n = self.gen.manifest().reltypes.len();
        let mut groups: Vec<(Val, u64)> = Vec::new();
        for t in 0..n as u32 {
            let Some(c) = count_for(t) else {
                return Ok(None); // marginal not present in this generation
            };
            if c > 0 {
                let name = self.gen.reltype_name(t).unwrap_or("").to_string();
                groups.push((Val::Str(name), c));
            }
        }
        Ok(Some(
            self.build_meta_result(sq, key_idx, count_idx, groups)?,
        ))
    }

    /// Recognise a whole-graph edge count — `MATCH ()-[r]->() RETURN count(*)` — and
    /// answer it from resident counts instead of walking the adjacency.
    ///
    /// Without this, the bare (ungrouped) edge count has no fast path at all: the grouped
    /// `RETURN type(r), count(*)` form is answered from the manifest, but dropping the
    /// group key sent the query to a full expansion — 96 s and a `maxScan` breach on a
    /// 1.5B-edge core. Merged views answer from the delta's edge counters, declining when
    /// those cannot be exact (see [`ReadView::live_edge_count`]).
    ///
    /// Declines on: a WHERE, a rel-type filter / rel property / variable length, any
    /// endpoint label or property, a self-loop pattern `(a)-[r]->(a)`, an undirected
    /// relationship (each edge would match in both orientations), extra pattern segments,
    /// a non-constant extra projection, or DISTINCT / ORDER BY / SKIP / LIMIT.
    pub(crate) fn try_edge_count_fast_path(
        &self,
        sq: &SingleQuery,
    ) -> Result<Option<(Vec<String>, Vec<Val>)>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if pat.segments.is_some() || pat.selector.is_some() || pat.restrictor.is_some() {
            return Ok(None);
        }
        if pat.rels.len() != 1 {
            return Ok(None);
        }
        let (rel, right) = &pat.rels[0];
        let left = &pat.start;
        if rel.type_expr.is_some() || !rel.props.is_empty() || rel.var_length.is_some() {
            return Ok(None);
        }
        // An undirected hop matches each edge in both orientations — not a plain count.
        if matches!(rel.dir, Direction::Undirected) {
            return Ok(None);
        }
        // Whole graph: both endpoints unconstrained, and not the same variable (which
        // would restrict the match to self-loops).
        if left.label_expr.is_some()
            || right.label_expr.is_some()
            || !left.props.is_empty()
            || !right.props.is_empty()
        {
            return Ok(None);
        }
        if let (Some(lv), Some(rv)) = (left.var.as_deref(), right.var.as_deref()) {
            if lv == rv {
                return Ok(None);
            }
        }

        let body = &sq.ret.body;
        if sq.ret.distinct
            || body.star
            || body.items.is_empty()
            || !body.order_by.is_empty()
            || body.skip.is_some()
            || body.limit.is_some()
        {
            return Ok(None);
        }
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_count_of(&it.expr, rel.var.as_deref()) {
                if count_idx.is_some() {
                    return Ok(None);
                }
                count_idx = Some(i);
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None);
            }
        }
        let Some(count_idx) = count_idx else {
            return Ok(None);
        };
        let Some(count) = self.gen.live_edge_count()? else {
            return Ok(None); // the delta cannot answer it exactly — run the matcher
        };
        Ok(Some(self.count_row(sq, count_idx, count as i64)?))
    }

    /// Recognise a whole-graph `labels(n)[0]` metadata query and answer it from the
    /// resident first-label counts, touching no blocks. Handles `MATCH (n) RETURN
    /// DISTINCT labels(n)[0]` and `RETURN labels(n)[0], count(*)`. Requires the
    /// generation's `first_label_counts` (so first-label semantics are reproduced
    /// exactly, even with multi-label nodes); the null bucket (zero-label nodes) is
    /// `node_count − Σ first_label_counts`. Declines on any node label/property
    /// constraint, a WHERE, a non-`[0]` index, extra non-constant projection,
    /// `count(DISTINCT …)`, or ORDER BY/SKIP/LIMIT.
    pub(crate) fn try_label_meta_fast_path(&self, sq: &SingleQuery) -> Result<Option<QueryResult>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if !pat.rels.is_empty() || pat.segments.is_some() {
            return Ok(None); // single-node pattern only
        }
        let node = &pat.start;
        if node.label_expr.is_some() || !node.props.is_empty() {
            return Ok(None); // whole-graph: no endpoint constraint
        }
        let Some(nodevar) = node.var.as_deref() else {
            return Ok(None);
        };
        // Requires exact first-label counts — otherwise first-label semantics can't
        // be reproduced from per-label occurrence counts under multi-label nodes.
        if !self.gen.has_first_label_counts() {
            return Ok(None);
        }
        // A core segment carries per-label *occurrence* deltas, not first-label deltas, so a
        // stacked set cannot reproduce `labels(n)[0]` groups from marginals — decline to full
        // execution (which reads the effective rows and is segment-aware).
        if !self.gen.core_stack().is_singleton() {
            return Ok(None);
        }

        let Some((key_idx, count_idx)) =
            self.classify_meta_projection(sq, |e| is_first_label_of(e, Some(nodevar)), nodevar)
        else {
            return Ok(None);
        };

        // Live groups: the core's first-label marginals, plus the delta's born nodes,
        // minus its suppressed rows. Zero-label nodes project `labels(n)[0] == null`.
        let groups: Vec<(Val, u64)> = self
            .gen
            .live_first_label_groups()?
            .into_iter()
            .map(|(name, c)| (name.map_or(Val::Null, Val::Str), c))
            .collect();
        Ok(Some(
            self.build_meta_result(sq, key_idx, count_idx, groups)?,
        ))
    }

    /// Shared projection guard for the metadata fast paths. Returns
    /// `Some((key_idx, count_idx))` when the RETURN is exactly one group key (matched
    /// by `is_key`) plus, for a grouped count, one `count(*)`/`count(var)` and any
    /// number of constant items — and the DISTINCT flag is consistent with that
    /// shape (enumeration must be DISTINCT; a grouped count must not be). `None`
    /// otherwise (the caller then declines). A trailing `ORDER BY` / `SKIP` / `LIMIT`
    /// is permitted — it is applied to the finished metadata rows in
    /// [`Self::build_meta_result`], exactly as the general path would.
    pub(crate) fn classify_meta_projection(
        &self,
        sq: &SingleQuery,
        is_key: impl Fn(&Expr) -> bool,
        countvar: &str,
    ) -> Option<(usize, Option<usize>)> {
        let body = &sq.ret.body;
        if body.star || body.items.is_empty() {
            return None;
        }
        let mut key_idx = None;
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_key(&it.expr) {
                if key_idx.is_some() {
                    return None;
                }
                key_idx = Some(i);
            } else if is_count_of(&it.expr, Some(countvar)) {
                if count_idx.is_some() {
                    return None;
                }
                count_idx = Some(i);
            } else if matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                // a constant grouping-neutral column (one group) — allowed
            } else {
                return None;
            }
        }
        let key_idx = key_idx?;
        // Enumeration (no count) must be DISTINCT, else it is a per-edge/-node
        // projection, not a metadata question. A grouped count must not be DISTINCT.
        match count_idx {
            None if !sq.ret.distinct => return None,
            Some(_) if sq.ret.distinct => return None,
            _ => {}
        }
        Some((key_idx, count_idx))
    }

    /// Assemble the single-column-per-projection-item result of a metadata fast path
    /// from the computed `(key_value, count)` groups, honouring the original item
    /// order (group key, optional count aggregate, and any constant items).
    pub(crate) fn build_meta_result(
        &self,
        sq: &SingleQuery,
        key_idx: usize,
        count_idx: Option<usize>,
        groups: Vec<(Val, u64)>,
    ) -> Result<QueryResult> {
        let items = &sq.ret.body.items;
        let columns: Vec<String> = items
            .iter()
            .map(|it| it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)))
            .collect();
        let empty: HashMap<String, Val> = HashMap::new();
        let mut rows = Vec::with_capacity(groups.len());
        for (key, count) in groups {
            let mut row = Vec::with_capacity(items.len());
            for (i, it) in items.iter().enumerate() {
                if i == key_idx {
                    row.push(key.clone());
                } else if Some(i) == count_idx {
                    row.push(Val::Int(count as i64));
                } else {
                    row.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
                }
            }
            rows.push(row);
        }
        // Apply any trailing ORDER BY / SKIP / LIMIT over the finished rows using the
        // same routine as the general projection, so an ordered/limited metadata query
        // is byte-identical to the scan (the rows are already the scan's answer).
        let rows = self.order_skip_limit_no_input(&sq.ret.body, &columns, rows)?;
        Ok(QueryResult { columns, rows })
    }

    /// Recognise a bare `RETURN count(*) | count(v)` over a single non-OPTIONAL
    /// `MATCH` (optional `WHERE`) whose pattern has relationships, and answer it by
    /// **counting matched rows during expansion** instead of materialising every
    /// completed binding. This is the multi-hop / WHERE sibling of
    /// [`Self::try_count_fast_path`] (which answers single-node counts from
    /// metadata) — it still walks, but never builds the row set, so a high-degree
    /// hub `count(*)` runs in O(1) result memory instead of the
    /// `query.maxIntermediate`-bounded `Vec<HashMap>` that is the fanout RSS peak.
    ///
    /// Guards (anything else returns `None` → the materialising path runs, still
    /// correct): one MATCH reading clause, non-OPTIONAL, no quantified/selector/
    /// restrictor pattern, at least one relationship; the RETURN is non-`*`, has no
    /// ORDER BY/SKIP/LIMIT, and its items are exactly one `count(*)`/`count(v)` (with
    /// `v` a variable this MATCH binds — always non-null on a completed non-OPTIONAL
    /// match, so `count(v) == count(*)`) plus any number of **constant** items.
    /// `count(DISTINCT …)`, `count(expr)`, a second aggregate, a grouping item, or a
    /// trailing clause all fall back (pushdown would miscount).
    pub(crate) fn try_count_walk_fast_path(&self, sq: &SingleQuery) -> Result<Option<QueryResult>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        // OPTIONAL emits outer-join rows whose pattern vars are null: count(v) would
        // then skip them (≠ count(*)) and count(*) over a no-match seed is 1, not 0.
        if m.optional {
            return Ok(None);
        }
        if m.patterns
            .iter()
            .any(|p| p.segments.is_some() || p.selector.is_some() || p.restrictor.is_some())
        {
            return Ok(None);
        }
        // Must actually walk — a pure single-node count is the metadata fast path's job.
        if m.patterns.iter().all(|p| p.rels.is_empty()) {
            return Ok(None);
        }

        let body = &sq.ret.body;
        if body.star
            || body.items.is_empty()
            || !body.order_by.is_empty()
            || body.skip.is_some()
            || body.limit.is_some()
        {
            return Ok(None);
        }

        // Variables this MATCH binds; each is non-null on a completed non-OPTIONAL
        // match, so `count(v)` over any of them equals `count(*)`.
        let mut bound: Vec<String> = Vec::new();
        for p in &m.patterns {
            collect_pattern_vars(p, &[], &mut bound);
        }

        // Exactly one count item; every other item a constant (one group).
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_count_star_or_var(&it.expr, &bound) {
                if count_idx.is_some() {
                    return Ok(None); // two counts — not our shape
                }
                count_idx = Some(i);
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None); // grouping key / other aggregate
            }
        }
        let Some(count_idx) = count_idx else {
            return Ok(None);
        };

        let n = self.count_match(m)?;

        // One output row: the count in its column, constants evaluated.
        let empty: HashMap<String, Val> = HashMap::new();
        let mut columns = Vec::with_capacity(body.items.len());
        let mut row = Vec::with_capacity(body.items.len());
        for (i, it) in body.items.iter().enumerate() {
            columns.push(it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)));
            if i == count_idx {
                row.push(Val::Int(n as i64));
            } else {
                row.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
            }
        }
        Ok(Some(QueryResult {
            columns,
            rows: vec![row],
        }))
    }

    /// Count the rows a non-OPTIONAL `MATCH` produces.
    ///
    /// For a single pattern with no `WHERE`, drive the ordinary matcher with the
    /// count accumulator armed: the chain-walk leaves tally completed rows and never
    /// materialise them (`out` stays empty), so a high-degree-hub `count(*)` runs in
    /// O(1) result memory — the fanout RSS win. Charging is unchanged, so the
    /// intermediate budget still bounds the walk exactly as before.
    ///
    /// A `WHERE` (the survivor filter is applied at the `match_patterns` terminal,
    /// after `match_single_pattern` has produced the rows) or a multi-pattern
    /// conjunction falls back to the materialising path — correct, just without the
    /// memory win (these are not the fanout-count hot shape).
    pub(crate) fn count_match(&self, m: &MatchClause) -> Result<u64> {
        if m.patterns.len() == 1 && m.where_.is_none() {
            debug_assert!(
                self.count_acc.get().is_none(),
                "count_match is not re-entrant"
            );
            self.count_acc.set(Some(0));
            let mut sink: Vec<HashMap<String, Val>> = Vec::new();
            let res =
                self.match_single_pattern(&m.patterns[0], &HashMap::new(), None, &mut sink, None);
            let n = self.count_acc.replace(None).unwrap_or(0);
            res?;
            debug_assert!(sink.is_empty(), "count-pushdown must not materialise rows");
            return Ok(n);
        }
        let table = self.apply_match(Table::singleton(), m, None)?;
        Ok(table.rows.len() as u64)
    }

    /// Recognise a single-node aggregation whose grouping/distinct key is an
    /// *indexed* property, and answer it from the range index instead of decoding
    /// the property from every node record. Returns the full `QueryResult` or
    /// `None` when any guard fails (the caller then executes normally).
    ///
    /// Guards mirror [`Self::try_count_fast_path`]: exactly one non-OPTIONAL
    /// `MATCH`, no `WHERE`, one single-node pattern (no rels), exactly one label,
    /// no inline props; the `RETURN` is non-DISTINCT and not `*`. The grouped /
    /// aggregated property must be a bare `n.p` with an open range index. Two
    /// shapes are recognised (anything else falls back):
    ///   - **group-by**: one `n.p` item + one `count(*)`/`count(n)` + any
    ///     constants → one row per distinct value of `p`, plus a null group for
    ///     nodes lacking `p` (`count(*)`/`count(n)` include nulls; `n` is never
    ///     null, so they agree).
    ///   - **distinct-count**: one `count(DISTINCT n.p)` + any constants, no
    ///     grouping item → a single row; the count is the number of distinct keys
    ///     (the index omits nulls, which `count(DISTINCT …)` also excludes).
    ///
    /// `ORDER BY`/`SKIP`/`LIMIT` are applied to the (small) grouped output via
    /// [`Self::order_skip_limit_no_input`].
    pub(crate) fn try_grouped_index_fast_path(
        &self,
        sq: &SingleQuery,
    ) -> Result<Option<QueryResult>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if !pat.rels.is_empty() || pat.segments.is_some() {
            return Ok(None); // single-node patterns only
        }
        let node = &pat.start;
        if !node.props.is_empty() {
            return Ok(None); // an inline prop is an extra equality filter
        }
        let Some(label) = node.label_expr.as_ref().and_then(|e| e.as_single_atom()) else {
            return Ok(None); // exactly one positive label (null-group denominator is exact)
        };
        let var = node.var.as_deref();

        let body = &sq.ret.body;
        // `sq.ret.distinct` is intentionally NOT a guard: `lower_return_clause`
        // sets it by scanning the clause text for the word "distinct", so
        // `RETURN count(DISTINCT n.p)` reports `ret.distinct = true` even though
        // there is no `RETURN DISTINCT`. For both shapes here the output rows are
        // unique by grouping key (the null group's key is distinct too), so a
        // final-row `DISTINCT` dedup is always a no-op — safe to ignore either way.
        if body.star || body.items.is_empty() {
            return Ok(None);
        }

        // Classify each RETURN item: a grouping property `n.p`, the (single)
        // count aggregate, or a constant. Anything else ⇒ fall back.
        let mut group_prop: Option<(usize, String)> = None;
        let mut count_plain: Option<usize> = None;
        let mut count_distinct: Option<(usize, String)> = None;
        for (i, it) in body.items.iter().enumerate() {
            if let Some(p) = node_property(&it.expr, var) {
                if group_prop.is_some() {
                    return Ok(None); // more than one grouping key
                }
                group_prop = Some((i, p));
            } else if is_count_of(&it.expr, var) {
                if count_plain.is_some() || count_distinct.is_some() {
                    return Ok(None);
                }
                count_plain = Some(i);
            } else if let Some(p) = count_distinct_property(&it.expr, var) {
                if count_plain.is_some() || count_distinct.is_some() {
                    return Ok(None);
                }
                count_distinct = Some((i, p));
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None); // a non-constant, non-{group,count} projection
            }
        }

        // Resolve the indexed property, the count column, and (group-by only) the
        // grouping column. Mixed shapes (e.g. `n.p, count(DISTINCT n.p)`) bail.
        let (prop, group_i, count_i, is_distinct) = match (group_prop, count_plain, count_distinct)
        {
            (Some((gi, p)), Some(ci), None) => (p, Some(gi), ci, false),
            (None, None, Some((ci, p))) => (p, None, ci, true),
            _ => return Ok(None),
        };

        let Some(idx_name) = index_for(self.gen, std::slice::from_ref(label), &prop) else {
            return Ok(None); // no open range index over (label, prop)
        };
        let reader = self
            .gen
            .range_index(&idx_name)
            .expect("index_for only returns open indexes");
        // Prefer the build-time histogram (O(distinct)); it is byte-identical to
        // `distinct_key_counts` (derived from this very index), so the answer is the
        // same. Absent (over the cardinality cap / pre-v3 generation) ⇒ walk the
        // index, exactly as before.
        let groups = match self.gen.property_histogram(&idx_name) {
            Some(h) => h.to_vec(),
            None => reader.distinct_key_counts()?,
        };

        let columns: Vec<String> = body
            .items
            .iter()
            .map(|it| it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)))
            .collect();
        let empty: HashMap<String, Val> = HashMap::new();

        let out_rows: Vec<Vec<Val>> = if is_distinct {
            // Single row: distinct non-null values = number of index keys.
            let n = groups.len() as i64;
            let mut r = Vec::with_capacity(body.items.len());
            for (i, it) in body.items.iter().enumerate() {
                r.push(if i == count_i {
                    Val::Int(n)
                } else {
                    self.eval(&it.expr, &Scope::Map(&empty), None)?
                });
            }
            vec![r]
        } else {
            let group_i = group_i.expect("group-by shape has a grouping item");
            let Some(lid) = self.gen.label_id(label) else {
                return Ok(None);
            };
            let total = self.gen.label_node_count(lid);
            let indexed: u64 = groups.iter().map(|(_, n)| *n).sum();
            let null_count = total.saturating_sub(indexed);

            let row_for = |gval: Val, count: i64| -> Result<Vec<Val>> {
                let mut r = Vec::with_capacity(body.items.len());
                for (i, it) in body.items.iter().enumerate() {
                    if i == group_i {
                        r.push(gval.clone());
                    } else if i == count_i {
                        r.push(Val::Int(count));
                    } else {
                        r.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
                    }
                }
                Ok(r)
            };

            let mut rows = Vec::with_capacity(groups.len() + 1);
            for (k, n) in groups {
                rows.push(row_for(Val::from_value(k), n as i64)?);
            }
            // Nodes of `label` that lack `prop` form the null group.
            if null_count > 0 {
                rows.push(row_for(Val::Null, null_count as i64)?);
            }
            rows
        };

        let rows = self.order_skip_limit_no_input(body, &columns, out_rows)?;
        Ok(Some(QueryResult { columns, rows }))
    }

    /// Apply a projection body's `ORDER BY` → `SKIP` → `LIMIT` to already-
    /// projected `rows` whose columns are `cols`. `ORDER BY` keys reference the
    /// projected aliases only — the aggregated / fast-path case, where there is no
    /// 1:1 input table to merge in (cf. the `with_input` branch in [`Self::project`]).
    pub(crate) fn order_skip_limit_no_input(
        &self,
        body: &ProjectionBody,
        cols: &[String],
        mut rows: Vec<Vec<Val>>,
    ) -> Result<Vec<Vec<Val>>> {
        if !body.order_by.is_empty() {
            // The `keyed` buffer clones every row plus its sort keys, so charge the
            // row count before building it (a large ORDER BY is otherwise uncharged).
            self.charge(rows.len() as u64)?;
            let mut keyed: Vec<(SortKey, Vec<Val>)> = Vec::with_capacity(rows.len());
            for r in rows {
                let scope = Scope::Row(cols, &r);
                let mut keys = Vec::with_capacity(body.order_by.len());
                for (e, dir) in &body.order_by {
                    keys.push((self.eval(e, &scope, None)?, *dir));
                }
                keyed.push((keys, r));
            }
            keyed.sort_by(|a, b| cmp_sort_keys(&a.0, &b.0));
            rows = keyed.into_iter().map(|(_, r)| r).collect();
        }
        if let Some(skip) = &body.skip {
            let n = self.eval_count(skip)?;
            rows = rows.into_iter().skip(n).collect();
        }
        if let Some(limit) = &body.limit {
            let n = self.eval_count(limit)?;
            rows.truncate(n);
        }
        Ok(rows)
    }

    /// Row cap a final `RETURN` lets us push into the last MATCH (root cause 6 —
    /// "buffer all paths"). When the projection is a plain 1:1 map — no
    /// aggregation, no `DISTINCT`, no `ORDER BY` — with a `LIMIT`, only the first
    /// `SKIP + LIMIT` matched rows (in match-emit order) can ever survive, so the
    /// match may stop the moment it has produced that many. Returns `None` when any
    /// of those needs the full set (aggregation/`DISTINCT`/`ORDER BY`, or no
    /// `LIMIT`). The pushdown is exact: stopping early yields the *same* prefix of
    /// rows that buffering-then-truncating does, since nothing between the match and
    /// the limit reorders or drops rows. `LIMIT`/`SKIP` are constant expressions
    /// (Cypher forbids row variables there), so evaluating them here is safe.
    pub(crate) fn projection_row_cap(
        &self,
        body: &ProjectionBody,
        distinct: bool,
    ) -> Result<Option<usize>> {
        let Some(limit) = &body.limit else {
            return Ok(None);
        };
        if distinct || !body.order_by.is_empty() {
            return Ok(None);
        }
        if body.items.iter().any(|it| contains_aggregate(&it.expr)) {
            return Ok(None);
        }
        let n = self.eval_count(limit)?;
        let skip = match &body.skip {
            Some(s) => self.eval_count(s)?,
            None => 0,
        };
        Ok(Some(n.saturating_add(skip)))
    }
}
