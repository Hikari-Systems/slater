// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for CALL: procedures, graph algorithms, subqueries and meta stats.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Run a read-only metadata procedure and bind its outputs into the table.
    /// The procedures take no arguments and produce rows independent of the input
    /// bindings, so the result is the input table × the procedure rows, projected
    /// to the `YIELD`ed columns (or all outputs when `YIELD` is absent) with the
    /// optional `YIELD … WHERE` applied. Mirrors [`Self::apply_vector_call`]'s
    /// binding/`WHERE` handling.
    pub(crate) fn apply_call(&self, table: Table, cc: &CallClause) -> Result<Table> {
        let lname = cc.name.to_ascii_lowercase();
        // algo.* graph-algorithm procedures take arguments (which may reference bound
        // variables) and compute their rows from the graph, so they follow the
        // per-row model of `apply_vector_call`, not the input-independent path below.
        if is_algo_proc(&lname) {
            return self.apply_algo_call(table, cc, &lname);
        }
        if !cc.args.is_empty() {
            bail!("{}() takes no arguments", cc.name);
        }
        let (out_names, proc_rows) = self.procedure_rows(&lname)?;

        // (output index, bound name) pairs: YIELD selects/reorders/aliases; a bare
        // call binds every output under its own name.
        let bindings: Vec<(usize, String)> = if cc.yields.is_empty() {
            out_names
                .iter()
                .enumerate()
                .map(|(i, n)| (i, n.clone()))
                .collect()
        } else {
            let mut v = Vec::with_capacity(cc.yields.len());
            for (output, bound) in &cc.yields {
                let idx = out_names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(output))
                    .ok_or_else(|| anyhow::anyhow!("{}() does not yield '{output}'", cc.name))?;
                v.push((idx, bound.clone()));
            }
            v
        };

        let mut out_cols = table.cols.clone();
        out_cols.extend(bindings.iter().map(|(_, b)| b.clone()));

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            for prow in &proc_rows {
                let mut r = row.clone();
                for (idx, _) in &bindings {
                    r.push(prow[*idx].clone());
                }
                if let Some(w) = &cc.where_ {
                    let scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &scope, None)?) != Some(true) {
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

    // ── algo.* graph-algorithm procedures (Phase 13) ─────────────────────────

    /// Per-row dispatch for an `algo.*` procedure: evaluate the arguments against
    /// each input row (they may reference bound variables, e.g. `algo.BFS(a, …)`),
    /// compute the procedure's rows from the graph, then cross-product input rows ×
    /// proc rows and bind the YIELD outputs. Mirrors [`Self::apply_vector_call`]'s
    /// per-row binding/`WHERE` handling; the proc rows carry every output in its
    /// canonical order, so a partial YIELD just selects a subset.
    pub(crate) fn apply_algo_call(
        &self,
        table: Table,
        cc: &CallClause,
        lname: &str,
    ) -> Result<Table> {
        let out_names = algo_outputs(lname);

        // (output index, bound name) pairs: YIELD selects/reorders/aliases; a bare
        // call binds every output under its own name (case-insensitive match).
        let bindings: Vec<(usize, String)> = if cc.yields.is_empty() {
            out_names
                .iter()
                .enumerate()
                .map(|(i, n)| (i, n.to_string()))
                .collect()
        } else {
            let mut v = Vec::with_capacity(cc.yields.len());
            for (output, bound) in &cc.yields {
                let idx = out_names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(output))
                    .ok_or_else(|| anyhow::anyhow!("{}() does not yield '{output}'", cc.name))?;
                v.push((idx, bound.clone()));
            }
            v
        };

        let mut out_cols = table.cols.clone();
        out_cols.extend(bindings.iter().map(|(_, b)| b.clone()));

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let args: Vec<Val> = cc
                .args
                .iter()
                .map(|e| self.eval(e, &scope, None))
                .collect::<Result<_>>()?;
            let proc_rows = self.algo_rows(lname, &args)?;
            for prow in &proc_rows {
                let mut r = row.clone();
                for (idx, _) in &bindings {
                    r.push(prow[*idx].clone());
                }
                if let Some(w) = &cc.where_ {
                    let row_scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &row_scope, None)?) != Some(true) {
                        continue;
                    }
                }
                // Each retained output row is charged, so an N-row input table crossed
                // with a per-row `algo.*` accumulates against the cumulative budget
                // rather than materialising for free.
                self.charge(1)?;
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// Compute the rows of an `algo.*` procedure for one set of evaluated arguments.
    /// Each row carries every output of the procedure in canonical order (see
    /// [`algo_outputs`]).
    pub(crate) fn algo_rows(&self, lname: &str, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        match lname {
            "algo.bfs" => self.algo_bfs(args),
            "algo.wcc" => self.algo_components(args),
            "algo.pagerank" => self.algo_pagerank(args),
            "algo.harmoniccentrality" => self.algo_harmonic(args),
            "algo.betweenness" => self.algo_betweenness(args),
            "algo.labelpropagation" => self.algo_labelprop(args),
            other => bail!("unknown procedure '{other}'"),
        }
    }

    /// `algo.BFS(source, maxLevel, relationshipType)` — single-source BFS, yielding
    /// one row `[nodes, edges]` of the reachable nodes (excluding the source) and the
    /// tree edge that first reached each. `maxLevel <= 0` is unlimited; a positive
    /// value caps the BFS depth. A NULL source, an unknown relationship type, or an
    /// unreachable source all produce **zero** rows (FalkorDB emits nothing).
    pub(crate) fn algo_bfs(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        if args.len() != 3 {
            bail!("algo.BFS expects 3 arguments (source, maxLevel, relationshipType)");
        }
        let source = match &args[0] {
            Val::Node(id) => *id,
            Val::Null => return Ok(Vec::new()),
            other => bail!("algo.BFS source must be a node, got {}", other.to_display()),
        };
        let max_level = match &args[1] {
            Val::Int(n) => *n,
            other => bail!(
                "algo.BFS maxLevel must be an integer, got {}",
                other.to_display()
            ),
        };
        let reltype: Option<u32> = match &args[2] {
            Val::Null => None,
            Val::Str(s) => match self.gen.reltype_id(s) {
                Some(id) => Some(id),
                None => return Ok(Vec::new()),
            },
            other => bail!(
                "algo.BFS relationshipType must be a string or null, got {}",
                other.to_display()
            ),
        };
        let unlimited = max_level <= 0;

        let mut visited = std::collections::HashSet::new();
        visited.insert(source);
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((source, 0i64));
        let mut nodes: Vec<Val> = Vec::new();
        let mut edges: Vec<Val> = Vec::new();
        // `nodes`/`edges`/`visited` grow over the reachable subgraph, so the loop is
        // both memory- and time-bounded here: `charge` caps the retained `Val`s
        // against `maxIntermediate` (two per discovered node — one `Val::Node`, one
        // `Val::Rel`), and a per-pop `check_deadline` makes a runaway
        // `algo.BFS(src, 0, NULL)` abort at `timeoutMs` rather than materialising tens
        // of millions of rows uninterruptibly. The deadline read is dwarfed by the
        // per-node `outgoing` block fetch, so an unconditional check is cheap enough
        // to guarantee prompt cancellation.
        while let Some((node, lvl)) = queue.pop_front() {
            self.check_deadline()?;
            if !unlimited && lvl >= max_level {
                continue;
            }
            for a in self.outgoing(node)? {
                if let Some(rt) = reltype {
                    if a.reltype != rt {
                        continue;
                    }
                }
                let nb = a.neighbour.0;
                if visited.insert(nb) {
                    // Charge before pushing so growth is bounded at the point of
                    // allocation, not after the whole subgraph is already resident.
                    self.charge(2)?;
                    nodes.push(Val::Node(nb));
                    edges.push(Val::Rel {
                        id: a.edge.0,
                        start: node,
                        end: nb,
                        reltype: a.reltype,
                    });
                    queue.push_back((nb, lvl + 1));
                }
            }
        }
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![vec![Val::List(nodes), Val::List(edges)]])
    }

    /// `algo.WCC([config])` — weakly-connected components; one row `[node,
    /// componentId]` per selected node. `componentId` is the smallest dense node id
    /// in the component (a stable canonical representative).
    pub(crate) fn algo_components(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, _) = self.parse_algo_config("WCC", args, &[])?;
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let roots = algo::wcc(view.nodes.len(), &view.undirected_edges(), &|| {
            self.check_deadline()
        })?;
        let group_id = canonical_group_ids(&view.nodes, &roots);
        Ok(view
            .nodes
            .iter()
            .enumerate()
            .map(|(i, &id)| vec![Val::Node(id), Val::Int(group_id[i])])
            .collect())
    }

    /// `algo.pageRank(label, relationshipType)` — PageRank over the (optionally
    /// label/reltype filtered) subgraph; one row `[node, score]` per selected node.
    /// The two arguments are scalar `string|null` (not a config map).
    pub(crate) fn algo_pagerank(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        if args.len() != 2 {
            bail!("algo.pageRank expects 2 arguments (label, relationshipType)");
        }
        let labels = self.scalar_label_filter("pageRank", &args[0])?;
        let rels = self.scalar_reltype_filter("pageRank", &args[1])?;
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let scores = algo::pagerank(view.nodes.len(), &view.out, &|| self.check_deadline())?;
        Ok(view
            .nodes
            .iter()
            .zip(scores)
            .map(|(&id, s)| vec![Val::Node(id), Val::Float(s)])
            .collect())
    }

    /// `algo.HarmonicCentrality([config])` — harmonic closeness; one row `[node,
    /// score, reachable]` per selected node.
    pub(crate) fn algo_harmonic(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, _) = self.parse_algo_config("HarmonicCentrality", args, &[])?;
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let hc = algo::harmonic(view.nodes.len(), &view.out, &|| self.check_deadline())?;
        Ok(view
            .nodes
            .iter()
            .zip(hc)
            .map(|(&id, (score, reach))| {
                vec![Val::Node(id), Val::Float(score), Val::Int(reach as i64)]
            })
            .collect())
    }

    /// `algo.betweenness([config])` — Brandes betweenness; one row `[node, score]`
    /// per selected node. `samplingSize`/`samplingSeed` are validated but ignored
    /// (the full exact betweenness is computed; see [`algo::betweenness`]).
    pub(crate) fn algo_betweenness(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, map) =
            self.parse_algo_config("betweenness", args, &["samplingSize", "samplingSeed"])?;
        if let Some(v) = map_get_ci(&map, "samplingSize") {
            if !matches!(v, Val::Int(n) if *n > 0) {
                bail!("betweenness configuration, 'samplingSize' should be a positive integer");
            }
        }
        if let Some(v) = map_get_ci(&map, "samplingSeed") {
            if !matches!(v, Val::Int(_)) {
                bail!("betweenness configuration, 'samplingSeed' should be an integer");
            }
        }
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let cb = algo::betweenness(view.nodes.len(), &view.out, &|| self.check_deadline())?;
        Ok(view
            .nodes
            .iter()
            .zip(cb)
            .map(|(&id, s)| vec![Val::Node(id), Val::Float(s)])
            .collect())
    }

    /// `algo.labelPropagation([config])` — CDLP community detection; one row `[node,
    /// communityId]` per selected node. `communityId` is the smallest dense node id
    /// in the community. `maxIterations` (default 10) caps the propagation rounds.
    pub(crate) fn algo_labelprop(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, map) =
            self.parse_algo_config("labelPropagation", args, &["maxIterations"])?;
        let mut max_iter = 10usize;
        if let Some(v) = map_get_ci(&map, "maxIterations") {
            match v {
                Val::Int(n) if *n > 0 => max_iter = *n as usize,
                _ => bail!(
                    "labelPropagation configuration, 'maxIterations' should be a positive integer"
                ),
            }
        }
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let comm = algo::cdlp(view.nodes.len(), &view.undirected_adj(), max_iter, &|| {
            self.check_deadline()
        })?;
        let group_id = canonical_group_ids(&view.nodes, &comm);
        Ok(view
            .nodes
            .iter()
            .enumerate()
            .map(|(i, &id)| vec![Val::Node(id), Val::Int(group_id[i])])
            .collect())
    }

    /// Parse the shared `algo.*` config-map argument (WCC / centrality / community
    /// procs). `args` holds 0 or 1 evaluated arguments; 0 args or a NULL argument is
    /// an empty config. `extra` lists the proc-specific keys permitted beyond
    /// `nodeLabels`/`relationshipTypes`. Returns the resolved label / reltype id
    /// filters (`None` = "all") plus the raw map for proc-specific keys. Unknown
    /// labels / reltypes are ignored (mirrors FalkorDB); unknown *keys* error.
    pub(crate) fn parse_algo_config(
        &self,
        proc: &str,
        args: &[Val],
        extra: &[&str],
    ) -> Result<AlgoConfig> {
        let map: Vec<(String, Val)> = match args {
            [] | [Val::Null] => Vec::new(),
            [Val::Map(m)] => m.clone(),
            [_] => bail!("invalid {proc} configuration"),
            _ => bail!("{proc} takes at most one configuration argument"),
        };
        for (k, _) in &map {
            let known = k.eq_ignore_ascii_case("nodeLabels")
                || k.eq_ignore_ascii_case("relationshipTypes")
                || extra.iter().any(|e| e.eq_ignore_ascii_case(k));
            if !known {
                bail!("{proc} configuration contains unknown key '{k}'");
            }
        }
        let labels = match map_get_ci(&map, "nodeLabels") {
            None => None,
            Some(v) => Some(self.resolve_name_filter(proc, "nodeLabels", v, true)?),
        };
        let rels = match map_get_ci(&map, "relationshipTypes") {
            None => None,
            Some(v) => Some(self.resolve_name_filter(proc, "relationshipTypes", v, false)?),
        };
        Ok((labels, rels, map))
    }

    /// Resolve a config `nodeLabels` / `relationshipTypes` value (must be an array of
    /// strings) to dense label / reltype ids, silently dropping names that don't
    /// exist in the schema.
    pub(crate) fn resolve_name_filter(
        &self,
        proc: &str,
        key: &str,
        v: &Val,
        is_label: bool,
    ) -> Result<Vec<u32>> {
        let items = match v {
            Val::List(xs) => xs,
            _ => bail!("{proc} configuration, '{key}' should be an array of strings"),
        };
        let mut ids = Vec::new();
        for it in items {
            let name = match it {
                Val::Str(s) => s,
                _ => bail!("{proc} configuration, '{key}' should be an array of strings"),
            };
            let id = if is_label {
                self.gen.label_id(name)
            } else {
                self.gen.reltype_id(name)
            };
            if let Some(id) = id {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    /// Resolve a scalar `string|null` label argument (algo.pageRank's first arg) to a
    /// single-label filter; `null` → `None` (all nodes). An unknown label yields an
    /// empty selection.
    pub(crate) fn scalar_label_filter(&self, proc: &str, v: &Val) -> Result<Option<Vec<u32>>> {
        match v {
            Val::Null => Ok(None),
            Val::Str(s) => Ok(Some(self.gen.label_id(s).into_iter().collect())),
            other => bail!(
                "algo.{proc} label must be a string or null, got {}",
                other.to_display()
            ),
        }
    }

    /// Resolve a scalar `string|null` relationship-type argument (algo.pageRank's
    /// second arg) to a single-reltype filter; `null` → `None` (all edges).
    pub(crate) fn scalar_reltype_filter(&self, proc: &str, v: &Val) -> Result<Option<Vec<u32>>> {
        match v {
            Val::Null => Ok(None),
            Val::Str(s) => Ok(Some(self.gen.reltype_id(s).into_iter().collect())),
            other => bail!(
                "algo.{proc} relationshipType must be a string or null, got {}",
                other.to_display()
            ),
        }
    }

    /// Materialise the filtered subgraph an `algo.*` procedure runs over: the
    /// selected dense node ids (ascending) plus directed out-adjacency as 0-based
    /// indices into that node list. `labels = None` selects every node; otherwise the
    /// union of nodes carrying any listed label. `rels = None` keeps every edge;
    /// otherwise only edges of a listed type. An edge is kept only when both
    /// endpoints are in the selected node set.
    pub(crate) fn build_view(
        &self,
        labels: Option<&[u32]>,
        rels: Option<&[u32]>,
    ) -> Result<GraphView> {
        // Route the node selection through `scan_candidates` so the view is built over the
        // *effective* estate: segment-born and delta-born nodes carrying a selected label are
        // included, tombstoned (segment or delta) ids are dropped, and a segment override that
        // changed a node's labels re-decides its membership. (This also folds the write-delta,
        // which the pre-segmented-core `build_view` ignored.)
        let nodes: Vec<u64> = match labels {
            None => self.scan_candidates(&NodeScan::AllNodes)?,
            Some(lbls) => {
                let mut set = std::collections::BTreeSet::new();
                for &l in lbls {
                    for nid in self.scan_candidates(&NodeScan::LabelScan { label_id: l })? {
                        set.insert(nid);
                    }
                }
                set.into_iter().collect()
            }
        };
        // The view (`nodes`, `pos`, and the `out` adjacency) is retained for the whole
        // algorithm run, so it is charged against `maxIntermediate` — an unfiltered
        // `algo.*` over a 91.6M-node store would otherwise OOM building the view before
        // any algorithm ran, ignoring the memory budget entirely. Charge the node-sized
        // structures first (`nodes` + `pos` + the `out` outer vec), so a huge selection
        // trips the budget before a single adjacency block is read.
        self.charge(nodes.len() as u64)?;
        let pos: HashMap<u64, usize> = nodes.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        // Each selected node's out-adjacency read is independent and touches only the
        // Sync cache, so gather the reads on the shared fanout pool (Task 11).
        // `neighbours_par` keeps the stored edge order and applies the same rel-type
        // filter, so mapping each neighbour through `pos` (single-threaded, `pos` is
        // shared read-only) yields the same 0-based index the sequential build did —
        // byte-for-byte identical node list + `out`. The gather is chunked so the
        // retained edge count is charged incrementally and the deadline is observed as
        // the view fills, bounding both memory and time before the algorithm starts;
        // concatenating chunks in order keeps the output identical to a single gather.
        let (gen, cache) = (self.gen, self.cache);
        let mut out: Vec<Vec<usize>> = Vec::with_capacity(nodes.len());
        for chunk in nodes.chunks(BUILD_VIEW_CHUNK) {
            self.check_deadline()?;
            let adj: Vec<Vec<u64>> = par_gather(
                self.fanout_pool.as_deref(),
                chunk,
                BUILD_VIEW_PAR_MIN,
                |&id| neighbours_par(gen, cache, id, Direction::Outgoing, rels),
            )?;
            let mut edges = 0u64;
            for nbs in &adj {
                let mapped: Vec<usize> = nbs.iter().filter_map(|nb| pos.get(nb).copied()).collect();
                edges += mapped.len() as u64;
                out.push(mapped);
            }
            // The retained `out` adjacency is walk work materialised for the run.
            self.charge(edges)?;
        }
        Ok(GraphView { nodes, out })
    }

    /// The fixed output columns and rows for a metadata procedure (lowercased name).
    pub(crate) fn procedure_rows(&self, name: &str) -> Result<(Vec<String>, Vec<Vec<Val>>)> {
        match name {
            // Slater enforces no constraints, so this is always empty — but with the
            // FalkorDB `db.constraints` output shape so a YIELD over it still binds.
            "db.constraints" => Ok((
                ["type", "label", "properties", "entitytype", "status"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                Vec::new(),
            )),
            "db.meta.stats" => Ok(self.meta_stats()),
            "dbms.procedures" => Ok(slater_procedures()),
            "dbms.functions" => Ok(slater_functions()),
            other => bail!("unknown procedure '{other}'"),
        }
    }

    /// `CALL db.meta.stats()` — schema/stat counts from the manifest plus the
    /// per-label / per-reltype count maps (ported from FalkorDB `proc_meta_stats.c`).
    /// All counts come from the resident per-label / per-reltype count maps
    /// (`label_node_count`, `reltype_edge_count`) — no graph scan.
    pub(crate) fn meta_stats(&self) -> (Vec<String>, Vec<Vec<Val>>) {
        let m = self.gen.manifest();
        // Live (delta- and stack-aware) counts. `live_label_node_count` / `live_node_count`
        // always sum; the edge/reltype live counts decline (→ base) when a segment's
        // marginals are not exact. All equal the base marginals for a singleton + empty
        // delta, so a pure-core `db.meta.stats()` is unchanged.
        let labels: Vec<(String, Val)> = m
            .labels
            .iter()
            .map(|l| {
                let cnt = self
                    .gen
                    .label_id(l)
                    .map(|id| self.gen.live_label_node_count(id).unwrap_or(0))
                    .unwrap_or(0);
                (l.clone(), Val::Int(cnt as i64))
            })
            .collect();
        let live_rt: Option<HashMap<String, u64>> = self
            .gen
            .live_reltype_edge_groups()
            .ok()
            .flatten()
            .map(|g| g.into_iter().collect());
        let reltypes: Vec<(String, Val)> = m
            .reltypes
            .iter()
            .map(|t| {
                let cnt = match &live_rt {
                    Some(map) => map.get(t).copied().unwrap_or(0),
                    None => self
                        .gen
                        .reltype_id(t)
                        .map(|id| self.gen.reltype_edge_count(id))
                        .unwrap_or(0),
                };
                (t.clone(), Val::Int(cnt as i64))
            })
            .collect();
        let node_count = self.gen.live_node_count();
        let edge_count = self
            .gen
            .live_edge_count()
            .ok()
            .flatten()
            .unwrap_or(m.edge_count);
        let cols = [
            "labels",
            "relTypes",
            "relCount",
            "nodeCount",
            "labelCount",
            "relTypeCount",
            "propertyKeyCount",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let row = vec![
            Val::Map(labels),
            Val::Map(reltypes),
            Val::Int(edge_count as i64),
            Val::Int(node_count as i64),
            Val::Int(m.labels.len() as i64),
            Val::Int(m.reltypes.len() as i64),
            Val::Int(m.property_keys.len() as i64),
        ];
        (cols, vec![row])
    }

    // ── CALL { … } subquery (Phase 12) ───────────────────────────────────────

    /// Run a correlated `CALL { … }` subquery: the inner query is executed once
    /// per outer row with its imported variables seeded, and the results are
    /// concatenated back. A returning subquery multiplies the outer cardinality by
    /// its result rows (each output row is `outer_row ++ inner_row`); a unit
    /// (`RETURN`-less) subquery passes the outer rows through unchanged (in a
    /// read-only engine it has no observable effect). Mirrors FalkorDB's
    /// `op_apply` + `op_argument`.
    pub(crate) fn apply_call_subquery(
        &self,
        table: Table,
        cs: &CallSubqueryClause,
    ) -> Result<Table> {
        // Unit subquery: run the inner clauses per row to surface any errors, then
        // emit the outer row unchanged (cardinality preserved).
        if !cs.returning {
            for row in &table.rows {
                self.check_deadline()?;
                self.run_subquery_for_row(cs, &table.cols, row)?;
            }
            return Ok(table);
        }

        let mut out_cols: Option<Vec<String>> = None;
        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let inner = self.run_subquery_for_row(cs, &table.cols, row)?;
            if out_cols.is_none() {
                out_cols = Some(self.subquery_out_cols(&table.cols, &inner.columns)?);
            }
            // Charge the cross-row buildup: one output row per (outer × inner) pair.
            self.charge(inner.rows.len() as u64)?;
            for irow in inner.rows {
                let mut r = row.clone();
                r.extend(irow);
                out_rows.push(r);
            }
        }
        // With no outer rows the inner never ran; derive the output schema from the
        // inner RETURN's projection names so the result still has correct columns.
        let cols = match out_cols {
            Some(c) => c,
            None => {
                let inner_cols: Vec<String> = cs
                    .inner
                    .head
                    .ret
                    .body
                    .items
                    .iter()
                    .map(|it| it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)))
                    .collect();
                self.subquery_out_cols(&table.cols, &inner_cols)?
            }
        };
        Ok(Table {
            cols,
            rows: out_rows,
        })
    }

    /// Combine the outer columns with the subquery's returned columns, rejecting a
    /// returned name that is already bound in the outer scope (FalkorDB
    /// "Variable `x` already declared in outer scope").
    pub(crate) fn subquery_out_cols(
        &self,
        outer: &[String],
        inner: &[String],
    ) -> Result<Vec<String>> {
        let mut cols = outer.to_vec();
        for ic in inner {
            if outer.iter().any(|c| c == ic) {
                bail!("Variable `{ic}` already declared in outer scope");
            }
            cols.push(ic.clone());
        }
        Ok(cols)
    }

    /// Execute the inner subquery (all `UNION` branches) for one outer row, each
    /// branch seeded with the variables it imports.
    pub(crate) fn run_subquery_for_row(
        &self,
        cs: &CallSubqueryClause,
        outer_cols: &[String],
        outer_row: &[Val],
    ) -> Result<QueryResult> {
        let head_seed = self.subquery_seed(&cs.imports[0], outer_cols, outer_row)?;
        let mut result = self.run_single_seeded(&cs.inner.head, head_seed)?;
        for (i, (union_all, part)) in cs.inner.tail.iter().enumerate() {
            let seed = self.subquery_seed(&cs.imports[i + 1], outer_cols, outer_row)?;
            let next = self.run_single_seeded(part, seed)?;
            if next.columns.len() != result.columns.len() {
                bail!("all branches of a CALL {{}} UNION must return the same number of columns");
            }
            self.charge(next.rows.len() as u64)?; // CALL{} UNION cross-branch buildup
            result.rows.extend(next.rows);
            if !*union_all {
                self.charge(result.rows.len() as u64)?; // DISTINCT `seen` set
                dedup_rows(&mut result.rows);
            }
        }
        Ok(result)
    }

    /// Build the one-row seed table that imports the requested outer variables into
    /// a subquery branch. `Imports::None` seeds the empty singleton (the subquery
    /// sees no outer variables); a named import that is not bound outside errors.
    pub(crate) fn subquery_seed(
        &self,
        imp: &Imports,
        outer_cols: &[String],
        outer_row: &[Val],
    ) -> Result<Table> {
        match imp {
            Imports::None => Ok(Table::singleton()),
            Imports::All => Ok(Table {
                cols: outer_cols.to_vec(),
                rows: vec![outer_row.to_vec()],
            }),
            Imports::Named(names) => {
                let mut row = Vec::with_capacity(names.len());
                for n in names {
                    let idx = outer_cols
                        .iter()
                        .position(|c| c == n)
                        .ok_or_else(|| anyhow::anyhow!("variable '{n}' is not in scope"))?;
                    row.push(outer_row[idx].clone());
                }
                Ok(Table {
                    cols: names.clone(),
                    rows: vec![row],
                })
            }
        }
    }

    // ── CALL db.idx.vector.queryNodes (brute-force KNN) ──────────────────────
}
