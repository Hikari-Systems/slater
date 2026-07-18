// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for projection and aggregation.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    pub(crate) fn project(
        &self,
        table: Table,
        body: &ProjectionBody,
        distinct: bool,
        post_where: Option<&Expr>,
    ) -> Result<Table> {
        // Expand `*` to the in-scope variables, then the explicit items.
        let mut items: Vec<(Expr, String)> = Vec::new();
        if body.star {
            for c in &table.cols {
                items.push((Expr::Var(c.clone()), c.clone()));
            }
        }
        for it in &body.items {
            let name = it.alias.clone().unwrap_or_else(|| expr_name(&it.expr));
            items.push((it.expr.clone(), name));
        }
        let out_cols: Vec<String> = items.iter().map(|(_, n)| n.clone()).collect();

        let aggregating = items.iter().any(|(e, _)| contains_aggregate(e));
        let mut out_rows = if aggregating {
            self.project_aggregated(&table, &items)?
        } else {
            self.project_simple(&table, &items)?
        };

        if distinct {
            self.charge(out_rows.len() as u64)?; // DISTINCT `seen` set
            dedup_rows(&mut out_rows);
        }

        // WITH ... WHERE (HAVING-style) filters the projected rows.
        if let Some(w) = post_where {
            let mut kept = Vec::new();
            for r in out_rows {
                if truthy(&self.eval(w, &Scope::Row(&out_cols, &r), None)?) {
                    kept.push(r);
                }
            }
            out_rows = kept;
        }

        // ORDER BY then SKIP then LIMIT. ORDER BY keys may reference the projected
        // aliases and (for a non-aggregated projection) the input row vars; the
        // alias wins on a clash.
        if !body.order_by.is_empty() {
            // The `keyed` buffer clones every row plus its sort keys, so charge the
            // row count before building it (a large ORDER BY is otherwise uncharged).
            self.charge(out_rows.len() as u64)?;
            let with_input = !aggregating && out_rows.len() == table.rows.len();
            let mut keyed: Vec<(SortKey, Vec<Val>)> = Vec::with_capacity(out_rows.len());
            for (i, r) in out_rows.into_iter().enumerate() {
                let out_scope = Scope::Row(&out_cols, &r);
                let mut keys = Vec::with_capacity(body.order_by.len());
                for (e, dir) in &body.order_by {
                    let v = if with_input {
                        let in_scope = Scope::Row(&table.cols, &table.rows[i]);
                        let merged = Scope::Merge(&out_scope, &in_scope);
                        self.eval(e, &merged, None)?
                    } else {
                        self.eval(e, &out_scope, None)?
                    };
                    keys.push((v, *dir));
                }
                keyed.push((keys, r));
            }
            keyed.sort_by(|a, b| cmp_sort_keys(&a.0, &b.0));
            out_rows = keyed.into_iter().map(|(_, r)| r).collect();
        }

        if let Some(skip) = &body.skip {
            let n = self.eval_count(skip)?;
            out_rows = out_rows.into_iter().skip(n).collect();
        }
        if let Some(limit) = &body.limit {
            let n = self.eval_count(limit)?;
            out_rows.truncate(n);
        }

        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    pub(crate) fn project_simple(
        &self,
        table: &Table,
        items: &[(Expr, String)],
    ) -> Result<Vec<Vec<Val>>> {
        let mut out = Vec::with_capacity(table.rows.len());
        for row in &table.rows {
            let scope = Scope::Row(&table.cols, row);
            let mut r = Vec::with_capacity(items.len());
            for (e, _) in items {
                r.push(self.eval(e, &scope, None)?);
            }
            out.push(r);
        }
        Ok(out)
    }

    pub(crate) fn project_aggregated(
        &self,
        table: &Table,
        items: &[(Expr, String)],
    ) -> Result<Vec<Vec<Val>>> {
        // Parallel fast path (Task 12): when every group key and aggregate argument
        // is `simple_readable` (Sync-evaluable: a var, literal, param, or `var.key`),
        // a fanout pool is configured, and the table is large enough, precompute the
        // per-row reads on the pool and reduce single-threaded. The grouping order,
        // budget charges and results are byte-for-byte identical to the sequential
        // body below — only the property reads move off-thread.
        if self.fanout_pool.is_some() && table.rows.len() >= AGG_PAR_MIN {
            if let Some((slots, plan)) = plan_par_aggregation(items) {
                if !slots.is_empty() {
                    return self.project_aggregated_par(table, items.len(), &slots, &plan);
                }
            }
        }
        // Grouping key = the values of the non-aggregating items, per row.
        let group_item: Vec<bool> = items.iter().map(|(e, _)| contains_aggregate(e)).collect();
        let mut groups: BTreeMap<GroupKey, Vec<usize>> = BTreeMap::new();
        for (ri, row) in table.rows.iter().enumerate() {
            let scope = Scope::Row(&table.cols, row);
            let mut key = Vec::new();
            for ((e, _), is_agg) in items.iter().zip(&group_item) {
                if !is_agg {
                    key.push(self.eval(e, &scope, None)?);
                }
            }
            // Charge each newly-created group: the `groups` map grows with the
            // distinct-key cardinality, which is otherwise uncharged (the per-row
            // index list is bounded by the already-charged input rows).
            let key = GroupKey(key);
            if !groups.contains_key(&key) {
                self.charge(1)?;
            }
            groups.entry(key).or_default().push(ri);
        }
        // An aggregation with no rows and no grouping keys still yields one row
        // (e.g. `RETURN count(*)` over an empty match → 0).
        if groups.is_empty() && !group_item.iter().any(|g| !g) {
            groups.insert(GroupKey(Vec::new()), Vec::new());
        }

        let mut out = Vec::with_capacity(groups.len());
        for (_, indices) in groups {
            // Representative row for grouping-key (non-agg) sub-expressions.
            let rep: &[Val] = indices.first().map(|&i| &table.rows[i][..]).unwrap_or(&[]);
            let rep_scope = Scope::Row(&table.cols, rep);
            let mut r = Vec::with_capacity(items.len());
            for (e, _) in items {
                if contains_aggregate(e) {
                    let mut aggs = Vec::new();
                    collect_aggregates(e, &mut aggs);
                    let mut vals = Vec::with_capacity(aggs.len());
                    for a in &aggs {
                        vals.push(self.compute_aggregate(a, table, &indices)?);
                    }
                    let cursor = AggCursor::new(vals);
                    r.push(self.eval(e, &rep_scope, Some(&cursor))?);
                } else {
                    r.push(self.eval(e, &rep_scope, None)?);
                }
            }
            out.push(r);
        }
        Ok(out)
    }

    /// Parallel counterpart to [`project_aggregated`](Self::project_aggregated) for the
    /// `simple_readable` shape (see [`plan_par_aggregation`]). The per-row group-key and
    /// aggregate-argument reads gather on the shared fanout pool (each touching only the
    /// Sync `gen`/`cache`); grouping, budget charges and the final reduction stay
    /// single-threaded in input order, so the output and the charge sequence are
    /// byte-for-byte identical to the sequential body.
    pub(crate) fn project_aggregated_par(
        &self,
        table: &Table,
        item_count: usize,
        slots: &[&Expr],
        plan: &[AggItem],
    ) -> Result<Vec<Vec<Val>>> {
        // Precompute, on the pool, the value of every slot expression for every row.
        // Capture only Sync state (never `&self`, which is `!Sync`).
        let gen = self.gen;
        let cache = self.cache;
        let params = &self.params;
        let cols = &table.cols;
        let cells: Vec<Vec<Val>> = par_gather(
            self.fanout_pool.as_deref(),
            &table.rows,
            AGG_PAR_MIN,
            |row| {
                slots
                    .iter()
                    .map(|e| eval_simple(gen, cache, params, cols, row, e))
                    .collect::<Result<Vec<_>>>()
            },
        )?;

        // Grouping: the key is the non-aggregate (Group) slots, in item order.
        let group_slots: Vec<usize> = plan
            .iter()
            .filter_map(|p| match p {
                AggItem::Group { slot } => Some(*slot),
                _ => None,
            })
            .collect();
        let has_group_item = !group_slots.is_empty();
        let mut groups: BTreeMap<GroupKey, Vec<usize>> = BTreeMap::new();
        for (ri, row) in cells.iter().enumerate() {
            let key = GroupKey(group_slots.iter().map(|&s| row[s].clone()).collect());
            if !groups.contains_key(&key) {
                self.charge(1)?; // charge each newly-created group (mirrors sequential)
            }
            groups.entry(key).or_default().push(ri);
        }
        // An aggregation with no rows and no grouping keys still yields one row.
        if groups.is_empty() && !has_group_item {
            groups.insert(GroupKey(Vec::new()), Vec::new());
        }

        let mut out = Vec::with_capacity(groups.len());
        for (_, indices) in groups {
            let mut r = Vec::with_capacity(item_count);
            for p in plan {
                match p {
                    // Grouping-key item: take the representative row's value (the
                    // first index — a Group item only exists when every group is
                    // non-empty, so `indices[0]` is always present).
                    AggItem::Group { slot } => r.push(cells[indices[0]][*slot].clone()),
                    AggItem::CountStar => r.push(Val::Int(indices.len() as i64)),
                    AggItem::Agg {
                        name,
                        distinct,
                        slot,
                    } => {
                        // Mirror `compute_aggregate`: drop nulls, charging each kept
                        // value in index order; for DISTINCT charge the dedup set, then
                        // reduce with the shared `reduce_agg`.
                        let mut vals = Vec::new();
                        for &i in &indices {
                            let v = cells[i][*slot].clone();
                            if !matches!(v, Val::Null) {
                                self.charge(1)?;
                                vals.push(v);
                            }
                        }
                        if *distinct {
                            self.charge(vals.len() as u64)?;
                            dedup_vals(&mut vals);
                        }
                        r.push(reduce_agg(name, vals)?);
                    }
                }
            }
            out.push(r);
        }
        Ok(out)
    }

    pub(crate) fn compute_aggregate(
        &self,
        agg: &Expr,
        table: &Table,
        indices: &[usize],
    ) -> Result<Val> {
        let Expr::Function {
            name,
            distinct,
            args,
        } = agg
        else {
            bail!("internal: compute_aggregate on a non-function");
        };
        let lname = name.to_lowercase();

        // count(*) needs no argument.
        if lname == "count" {
            if let FuncArgs::Star = args {
                return Ok(Val::Int(indices.len() as i64));
            }
        }
        let args_slice = match args {
            FuncArgs::Args(a) => a.as_slice(),
            FuncArgs::Star => bail!("aggregate {name} expects an argument"),
        };

        // `percentileCont`/`percentileDisc` are two-arg aggregates: the first arg
        // is collected per row, the second is a constant percentile in [0, 1]
        // that FalkorDB reads once on the first invocation. Extract it before the
        // per-row loop (evaluated against a representative row of the group).
        let is_percentile = matches!(lname.as_str(), "percentilecont" | "percentiledisc");
        let percentile = if is_percentile {
            if args_slice.len() != 2 {
                bail!("{name}() expects exactly two arguments");
            }
            let pscope = match indices.first() {
                Some(&i) => Scope::Row(&table.cols, &table.rows[i]),
                None => Scope::Empty,
            };
            let p = match self.eval(&args_slice[1], &pscope, None)?.as_num() {
                Some(p) => p,
                None => bail!("{name}() percentile must be a number"),
            };
            if !(0.0..=1.0).contains(&p) {
                bail!("Percentile value must be in the range 0 to 1, got {p}");
            }
            Some(p)
        } else {
            if args_slice.len() != 1 {
                bail!("aggregate {name} expects exactly one argument");
            }
            None
        };
        let arg = &args_slice[0];

        // Evaluate the argument over the group's rows, dropping nulls. The buffer
        // is materialised for every aggregate (not just collect), so it charges
        // the intermediate budget.
        let mut vals = Vec::new();
        for &i in indices {
            let scope = Scope::Row(&table.cols, &table.rows[i]);
            let v = self.eval(arg, &scope, None)?;
            if !matches!(v, Val::Null) {
                self.charge(1)?;
                vals.push(v);
            }
        }
        if *distinct {
            self.charge(vals.len() as u64)?; // DISTINCT-aggregate `seen` set
            dedup_vals(&mut vals);
        }

        // `percentile*` carry the constant percentile; every other aggregate's
        // value→result reduction is shared with the parallel path via `reduce_agg`.
        match lname.as_str() {
            "percentilecont" => percentile_cont(&vals, percentile.unwrap()),
            "percentiledisc" => percentile_disc(&vals, percentile.unwrap()),
            other => reduce_agg(other, vals),
        }
    }

    /// Evaluate a constant `SKIP`/`LIMIT` expression to a non-negative count.
    pub(crate) fn eval_count(&self, e: &Expr) -> Result<usize> {
        match self.eval(e, &Scope::Empty, None)? {
            Val::Int(n) if n >= 0 => Ok(n as usize),
            other => bail!("SKIP/LIMIT must be a non-negative integer, got {other:?}"),
        }
    }

    // ── Expression evaluation ───────────────────────────────────────────────
}
