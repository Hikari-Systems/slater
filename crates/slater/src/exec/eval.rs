// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for expression evaluation and scalar-function dispatch.
//!
//! Split out of `exec.rs` as a child module — a pure relocation, no logic changed.
//! Methods reach the `Engine` struct, its private fields and the shared free
//! helpers through `use super::*`; cross-module calls are `pub(crate)`.

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    pub(crate) fn eval(&self, expr: &Expr, scope: &Scope, aggs: Option<&AggCursor>) -> Result<Val> {
        match expr {
            Expr::Literal(v) => Ok(Val::from_value(v.clone())),
            Expr::Param(name) => self
                .params
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("parameter ${name} was not supplied")),
            Expr::Var(name) => scope
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("variable '{name}' is not in scope")),
            Expr::Property(base, key) => {
                let b = self.eval(base, scope, aggs)?;
                self.property(&b, key)
            }
            Expr::Index(base, idx) => {
                let b = self.eval(base, scope, aggs)?;
                let i = self.eval(idx, scope, aggs)?;
                self.index(&b, &i)
            }
            Expr::Slice { base, from, to } => {
                let b = self.eval(base, scope, aggs)?;
                // Absent bounds default to 0 / INT32_MAX, mirroring FalkorDB's
                // slice AST construction; an explicit NULL bound yields NULL.
                let f = match from {
                    Some(e) => self.eval(e, scope, aggs)?,
                    None => Val::Int(0),
                };
                let t = match to {
                    Some(e) => self.eval(e, scope, aggs)?,
                    None => Val::Int(i32::MAX as i64),
                };
                self.slice(&b, &f, &t)
            }
            Expr::HasLabels(base, labels) => {
                let b = self.eval(base, scope, aggs)?;
                self.has_labels(&b, labels)
            }
            Expr::Neg(e) => match self.eval(e, scope, aggs)? {
                // `-i64::MIN` has no `i64` value (the two's-complement range is
                // asymmetric), so negation is checked like every other integer op —
                // it wrapped back to `i64::MIN` in release before this.
                Val::Int(i) => match i.checked_neg() {
                    Some(v) => Ok(Val::Int(v)),
                    None => bail!(ArithmeticOverflow::unary("-", i)),
                },
                Val::Float(f) => Ok(Val::Float(-f)),
                Val::Null => Ok(Val::Null),
                other => bail!("cannot negate {}", other.to_display()),
            },
            Expr::Not(e) => Ok(match three_valued(&self.eval(e, scope, aggs)?) {
                Some(b) => Val::Bool(!b),
                None => Val::Null,
            }),
            Expr::And(parts) => self.fold_bool(parts, scope, aggs, BoolOp::And),
            Expr::Or(parts) => self.fold_bool(parts, scope, aggs, BoolOp::Or),
            Expr::Xor(parts) => self.fold_bool(parts, scope, aggs, BoolOp::Xor),
            Expr::Arith(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                let v = arith(*op, a, b)?;
                // List concatenation is the only arithmetic that materialises a
                // collection; charging every temp defeats geometric growth like
                // `reduce(acc = [0], x IN range(1, 60) | acc + acc)`.
                if let Val::List(xs) = &v {
                    self.charge(xs.len() as u64)?;
                }
                Ok(v)
            }
            Expr::Compare(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                Ok(compare(*op, &a, &b))
            }
            Expr::StringOp(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                self.string_op(*op, &a, &b)
            }
            Expr::In(l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                Ok(in_list(&a, &b))
            }
            Expr::IsNull(e, negated) => {
                let v = self.eval(e, scope, aggs)?;
                let is_null = matches!(v, Val::Null);
                Ok(Val::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Case {
                subject,
                whens,
                els,
            } => self.eval_case(subject, whens, els, scope, aggs),
            Expr::Function {
                name,
                distinct,
                args,
            } => {
                if is_aggregate(name) {
                    return match aggs {
                        Some(cursor) => Ok(cursor.next()),
                        None => bail!("aggregation '{name}' is not allowed here"),
                    };
                }
                let arg_vals = match args {
                    FuncArgs::Star => bail!("'{name}(*)' is only valid for count"),
                    FuncArgs::Args(a) => a
                        .iter()
                        .map(|e| self.eval(e, scope, aggs))
                        .collect::<Result<Vec<_>>>()?,
                };
                self.call_function(name, *distinct, arg_vals)
            }
            Expr::List(items) => Ok(Val::List(
                items
                    .iter()
                    .map(|e| self.eval(e, scope, aggs))
                    .collect::<Result<_>>()?,
            )),
            Expr::Map(entries) => {
                let mut m = Vec::with_capacity(entries.len());
                for (k, e) in entries {
                    m.push((k.clone(), self.eval(e, scope, aggs)?));
                }
                Ok(Val::Map(m))
            }
            Expr::MapProjection { var, items } => self.eval_map_projection(var, items, scope, aggs),
            Expr::ListPredicate {
                quant,
                var,
                list,
                predicate,
            } => self.eval_list_predicate(*quant, var, list, predicate.as_deref(), scope, aggs),
            Expr::ListComprehension {
                var,
                list,
                predicate,
                projection,
            } => self.eval_list_comprehension(
                var,
                list,
                predicate.as_deref(),
                projection.as_deref(),
                scope,
                aggs,
            ),
            Expr::PatternComprehension {
                pattern,
                predicate,
                projection,
            } => self.eval_pattern_comprehension(
                pattern,
                predicate.as_deref(),
                projection,
                scope,
                aggs,
            ),
            Expr::Reduce {
                acc_var,
                acc_init,
                var,
                list,
                body,
            } => self.eval_reduce(acc_var, acc_init, var, list, body, scope, aggs),
            Expr::PatternPredicate(pattern) => {
                // True iff the pattern, seeded by the current bindings, has ≥1
                // match (FalkorDB `op_semi_apply`; the negated form `NOT (…)` is
                // anti-semi-apply via the surrounding `Expr::Not`). No early-exit:
                // all matches are collected, then emptiness is tested.
                let seed = scope.to_binding();
                let mut bindings = Vec::new();
                self.match_single_pattern(pattern, &seed, None, &mut bindings, None)?;
                Ok(Val::Bool(!bindings.is_empty()))
            }
            Expr::Exists {
                patterns,
                predicate,
            } => {
                // `match_patterns` seeds from the outer bindings, chains the
                // comma-separated patterns, and applies the inner WHERE once every
                // pattern is bound — exactly the semi-apply existence test.
                let seed = scope.to_binding();
                let mut bindings = Vec::new();
                self.match_patterns(patterns, 0, seed, predicate.as_deref(), &mut bindings, None)?;
                Ok(Val::Bool(!bindings.is_empty()))
            }
            Expr::ShortestPath(pattern) => self.eval_shortest_path(pattern, scope),
        }
    }

    /// Evaluate `shortestPath((a)-[*]->(b))` against the current scope: a BFS over
    /// the traversal adjacency from the bound source to the bound destination,
    /// returning the first (hence shortest) connecting [`Val::Path`], or `Val::Null`
    /// when none exists. Mirrors FalkorDB's validation of the wrapped pattern.
    pub(crate) fn eval_shortest_path(&self, pattern: &Pattern, scope: &Scope) -> Result<Val> {
        // The inner pattern must be a single variable-length relationship with no
        // property filter, between two endpoints already bound to nodes.
        if pattern.rels.len() != 1 {
            bail!("shortestPath requires a path containing a single relationship");
        }
        let (rel, end) = &pattern.rels[0];
        if !rel.props.is_empty() {
            bail!("filters on relationships in shortestPath are not supported");
        }
        let (min, max) = match &rel.var_length {
            Some(vl) => varlen_bounds(vl),
            None => (1, 1),
        };
        if min > 1 {
            bail!("shortestPath does not support a minimal length different from 0 or 1");
        }
        let bound_node = |var: Option<&str>| -> Result<u64> {
            match var.and_then(|v| scope.get(v)) {
                Some(Val::Node(id)) => Ok(id),
                _ => bail!("A shortestPath requires bound nodes"),
            }
        };
        let src = bound_node(pattern.start.var.as_deref())?;
        let dst = bound_node(end.var.as_deref())?;
        // FalkorDB orients the returned path from the relationship arrow's tail to
        // its head. The shared core walks the syntactic start→end (using the
        // pattern's direction); for an incoming pattern `(b)<-[*]-(a)` the arrow tail
        // is the end node, so the result is reversed into arrow order. (Undirected
        // keeps start→end order.)
        let reverse = matches!(rel.dir, Direction::Incoming);

        // Delegate to the shared selector core: `shortestPath()` is exactly
        // `ANY SHORTEST` between two bound nodes — one loopless shortest path, or none.
        let Some(hops) = self
            .select_paths(src, dst, rel, (min, max), PathSelector::AnyShortest)?
            .into_iter()
            .next()
        else {
            return Ok(Val::Null);
        };
        let path = make_path(src, &hops);
        if reverse {
            if let Val::Path {
                mut nodes,
                mut rels,
            } = path
            {
                nodes.reverse();
                rels.reverse();
                return Ok(Val::Path { nodes, rels });
            }
        }
        Ok(path)
    }

    pub(crate) fn fold_bool(
        &self,
        parts: &[Expr],
        scope: &Scope,
        aggs: Option<&AggCursor>,
        op: BoolOp,
    ) -> Result<Val> {
        let mut saw_null = false;
        let mut acc = matches!(op, BoolOp::And); // identity: AND=true, OR/XOR=false
        for p in parts {
            match three_valued(&self.eval(p, scope, aggs)?) {
                Some(b) => match op {
                    BoolOp::And => {
                        if !b {
                            return Ok(Val::Bool(false));
                        }
                    }
                    BoolOp::Or => {
                        if b {
                            return Ok(Val::Bool(true));
                        }
                    }
                    BoolOp::Xor => acc ^= b,
                },
                None => saw_null = true,
            }
        }
        if saw_null {
            return Ok(Val::Null);
        }
        Ok(Val::Bool(acc))
    }

    // Point coordinate read (FalkorDB `Point_GetCoordinate`): only
    // `latitude`/`longitude` resolve; any other key yields NULL. Temporal component
    // access (FalkorDB `entity_funcs.c` → `*_getComponent`): an unknown component is
    // an *error* (unlike Point/Map, which yield NULL). The body lives in the Sync
    // free fn [`property_val`] so the parallel aggregation precompute can share it.
    pub(crate) fn property(&self, base: &Val, key: &str) -> Result<Val> {
        property_val(self.gen, self.cache, base, key)
    }

    pub(crate) fn index(&self, base: &Val, idx: &Val) -> Result<Val> {
        match (base, idx) {
            (Val::Null, _) | (_, Val::Null) => Ok(Val::Null),
            (Val::List(xs), Val::Int(i)) => Ok(list_index(xs.len(), *i)
                .map(|n| xs[n].clone())
                .unwrap_or(Val::Null)),
            (Val::Vector(xs), Val::Int(i)) => Ok(list_index(xs.len(), *i)
                .map(|n| Val::Float(xs[n] as f64))
                .unwrap_or(Val::Null)),
            (Val::Map(m), Val::Str(k)) => Ok(m
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
                .unwrap_or(Val::Null)),
            _ => bail!(
                "cannot index {} with {}",
                base.to_display(),
                idx.to_display()
            ),
        }
    }

    /// `base[from..to]` slice. Mirrors FalkorDB `AR_SLICE`: any NULL operand
    /// yields NULL; a negative bound counts from the end (clamped into range); a
    /// non-positive width yields an empty result. Extends FalkorDB (arrays only)
    /// to strings, slicing by Unicode scalar value.
    pub(crate) fn slice(&self, base: &Val, from: &Val, to: &Val) -> Result<Val> {
        if matches!(base, Val::Null) || matches!(from, Val::Null) || matches!(to, Val::Null) {
            return Ok(Val::Null);
        }
        let start = num_i64(Some(from))?;
        let end = num_i64(Some(to))?;
        match base {
            Val::List(xs) => Ok(Val::List(slice_range(xs, start, end).to_vec())),
            Val::Vector(xs) => Ok(Val::List(
                slice_range(xs, start, end)
                    .iter()
                    .map(|f| Val::Float(*f as f64))
                    .collect(),
            )),
            Val::Str(s) => {
                let chars: Vec<char> = s.chars().collect();
                Ok(Val::Str(slice_range(&chars, start, end).iter().collect()))
            }
            other => bail!("cannot slice {}", other.to_display()),
        }
    }

    pub(crate) fn has_labels(&self, base: &Val, labels: &[String]) -> Result<Val> {
        match base {
            Val::Null => Ok(Val::Null),
            Val::Node(id) => {
                let have = self.node_label_ids(*id)?;
                for l in labels {
                    match self.gen.label_id(l) {
                        Some(lid) if have.contains(&lid) => {}
                        _ => return Ok(Val::Bool(false)),
                    }
                }
                Ok(Val::Bool(true))
            }
            other => bail!("cannot test labels on {}", other.to_display()),
        }
    }

    pub(crate) fn eval_case(
        &self,
        subject: &Option<Box<Expr>>,
        whens: &[(Expr, Expr)],
        els: &Option<Box<Expr>>,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        match subject {
            // Simple form: CASE x WHEN v THEN ... — compare x to each v.
            Some(subj) => {
                let s = self.eval(subj, scope, aggs)?;
                for (v, then) in whens {
                    let cand = self.eval(v, scope, aggs)?;
                    if s.loose_eq(&cand) == Some(true) {
                        return self.eval(then, scope, aggs);
                    }
                }
            }
            // Searched form: CASE WHEN cond THEN ... — first true condition.
            None => {
                for (cond, then) in whens {
                    if truthy(&self.eval(cond, scope, aggs)?) {
                        return self.eval(then, scope, aggs);
                    }
                }
            }
        }
        match els {
            Some(e) => self.eval(e, scope, aggs),
            None => Ok(Val::Null),
        }
    }

    pub(crate) fn eval_map_projection(
        &self,
        var: &str,
        items: &[MapProjItem],
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let base = scope
            .get(var)
            .ok_or_else(|| anyhow::anyhow!("variable '{var}' is not in scope"))?;
        let mut out: Vec<(String, Val)> = Vec::new();
        for item in items {
            match item {
                MapProjItem::AllProps => {
                    for (k, v) in self.all_properties(&base)? {
                        out.push((k, v));
                    }
                }
                MapProjItem::Property(p) => out.push((p.clone(), self.property(&base, p)?)),
                MapProjItem::Literal(k, e) => out.push((k.clone(), self.eval(e, scope, aggs)?)),
            }
        }
        Ok(Val::Map(out))
    }

    pub(crate) fn all_properties(&self, base: &Val) -> Result<Vec<(String, Val)>> {
        match base {
            Val::Node(id) => {
                // Core-stack row (segment or base) in name space, then the delta overlay.
                let mut out = self.core_named_props(*id)?;
                self.overlay_node_props(*id, &mut out);
                let labels: Vec<String> = self
                    .node_label_ids(*id)?
                    .into_iter()
                    .filter_map(|l| self.gen.label_name(l).map(|s| s.to_string()))
                    .collect();
                self.suppress_indexed_vectors_named(&labels, &mut out);
                Ok(out)
            }
            // `core_named_edge_props` already folds the segment row and the delta patches.
            Val::Rel { id, .. } => self.core_named_edge_props(*id),
            Val::Map(m) => Ok(m.clone()),
            Val::Null => Ok(Vec::new()),
            other => bail!("type {} has no properties", other.to_display()),
        }
    }

    pub(crate) fn eval_list_predicate(
        &self,
        quant: Quantifier,
        var: &str,
        list: &Expr,
        predicate: Option<&Expr>,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let items = match self.eval(list, scope, aggs)? {
            Val::List(xs) => xs,
            Val::Null => return Ok(Val::Null),
            other => bail!("a list predicate needs a list, got {}", other.to_display()),
        };
        let mut true_count = 0usize;
        let mut any_false = false;
        let mut saw_null = false;
        for item in &items {
            let inner = Scope::With(scope, var, item);
            let v = match predicate {
                Some(p) => self.eval(p, &inner, aggs)?,
                None => item.clone(),
            };
            match three_valued(&v) {
                Some(true) => true_count += 1,
                Some(false) => any_false = true,
                None => saw_null = true,
            }
        }
        Ok(match quant {
            // any: a definite true wins; else null if any null; else false.
            Quantifier::Any => {
                if true_count > 0 {
                    Val::Bool(true)
                } else if saw_null {
                    Val::Null
                } else {
                    Val::Bool(false)
                }
            }
            // all: a definite false wins; else null if any null; else true.
            Quantifier::All => {
                if any_false {
                    Val::Bool(false)
                } else if saw_null {
                    Val::Null
                } else {
                    Val::Bool(true)
                }
            }
            // none: a definite true → false; else null if any null; else true.
            Quantifier::None => {
                if true_count > 0 {
                    Val::Bool(false)
                } else if saw_null {
                    Val::Null
                } else {
                    Val::Bool(true)
                }
            }
            Quantifier::Single => Val::Bool(true_count == 1),
        })
    }

    /// Evaluate `[var IN list WHERE predicate | projection]`. Mirrors
    /// [`Self::eval_list_predicate`] element binding: iterate the source list with
    /// `var` layered onto the scope, keep elements whose predicate is definitely
    /// true (a NULL predicate excludes, like FalkorDB's three-valued filter), and
    /// project each survivor (defaulting to the bound element). A NULL source list
    /// yields NULL.
    pub(crate) fn eval_list_comprehension(
        &self,
        var: &str,
        list: &Expr,
        predicate: Option<&Expr>,
        projection: Option<&Expr>,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let items = match self.eval(list, scope, aggs)? {
            Val::List(xs) => xs,
            Val::Null => return Ok(Val::Null),
            other => bail!(
                "a list comprehension needs a list, got {}",
                other.to_display()
            ),
        };
        let mut out = Vec::new();
        for item in &items {
            let inner = Scope::With(scope, var, item);
            if let Some(p) = predicate {
                if !truthy(&self.eval(p, &inner, aggs)?) {
                    continue;
                }
            }
            self.charge(1)?;
            out.push(match projection {
                Some(e) => self.eval(e, &inner, aggs)?,
                None => item.clone(),
            });
        }
        Ok(Val::List(out))
    }

    /// Evaluate `reduce(acc = init, var IN list | body)`. Mirrors FalkorDB
    /// `AR_REDUCE`: a NULL list yields NULL; otherwise fold `body` over the list,
    /// threading the accumulator (seeded from `init`) and binding `var` to each
    /// element. Both bindings shadow the surrounding scope only inside `body`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn eval_reduce(
        &self,
        acc_var: &str,
        acc_init: &Expr,
        var: &str,
        list: &Expr,
        body: &Expr,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let items = match self.eval(list, scope, aggs)? {
            Val::List(xs) => xs,
            Val::Null => return Ok(Val::Null),
            other => bail!("reduce() needs a list, got {}", other.to_display()),
        };
        let mut acc = self.eval(acc_init, scope, aggs)?;
        for item in &items {
            let acc_scope = Scope::With(scope, acc_var, &acc);
            let inner = Scope::With(&acc_scope, var, item);
            acc = self.eval(body, &inner, aggs)?;
        }
        Ok(acc)
    }

    /// Evaluate `[pattern WHERE predicate | projection]`. The pattern is matched
    /// against the surrounding scope (its already-bound nodes seed the traversal),
    /// the optional `WHERE` filters matches, and `projection` is collected per
    /// match in match order. New pattern variables stay local to each match and do
    /// not leak to the outer row; an empty match set yields `[]`. This is the
    /// observable equivalent of FalkorDB's correlated collect sub-plan.
    pub(crate) fn eval_pattern_comprehension(
        &self,
        pattern: &Pattern,
        predicate: Option<&Expr>,
        projection: &Expr,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let seed = scope.to_binding();
        let mut bindings = Vec::new();
        self.match_single_pattern(pattern, &seed, predicate, &mut bindings, None)?;
        let mut out = Vec::with_capacity(bindings.len());
        for b in bindings {
            out.push(self.eval(projection, &Scope::Map(&b), aggs)?);
        }
        Ok(Val::List(out))
    }

    pub(crate) fn call_function(&self, name: &str, _distinct: bool, args: Vec<Val>) -> Result<Val> {
        let n = name.to_lowercase();
        let a0 = |i: usize| args.get(i).cloned().unwrap_or(Val::Null);
        Ok(match n.as_str() {
            "coalesce" => args
                .into_iter()
                .find(|v| !matches!(v, Val::Null))
                .unwrap_or(Val::Null),
            // Pure, deterministic scalar functions whose result depends only on
            // their argument values are single-sourced in the `slater-scalar`
            // crate (shared with the offline builder). Convert the runtime args to
            // on-disk `Value`s and delegate; a runtime-only argument (node / map /
            // path / temporal) can never satisfy these string/numeric/conversion
            // functions and yields NULL — exactly the old `str_fn`/`num_fn`
            // behaviour for a non-scalar argument.
            "tolower" | "lower" | "toupper" | "upper" | "trim" | "ltrim" | "rtrim"
            | "tointeger" | "tointegerornull" | "tofloat" | "tofloatornull" | "toboolean"
            | "tobooleanornull" | "abs" | "ceil" | "floor" | "round" | "sqrt" | "log" | "log10"
            | "exp" | "e" | "pi" | "pow" | "sign" | "sin" | "cos" | "tan" | "cot" | "asin"
            | "acos" | "atan" | "atan2" | "degrees" | "radians" | "haversin" => {
                match try_all_values(&args) {
                    Some(vs) => Val::from_value(
                        slater_scalar::eval_pure(&n, &vs)?
                            .expect("listed name is handled by slater-scalar"),
                    ),
                    None => Val::Null,
                }
            }
            "reverse" => match a0(0) {
                Val::Str(s) => Val::Str(s.chars().rev().collect()),
                Val::List(mut xs) => {
                    xs.reverse();
                    Val::List(xs)
                }
                Val::Null => Val::Null,
                other => bail!(
                    "reverse() needs a string or list, got {}",
                    other.to_display()
                ),
            },
            // `length(path)` is the relationship count (FalkorDB `AR_PATH_LENGTH`);
            // `size`/`length` over a collection/string is the element/char count.
            "size" | "length" => match a0(0) {
                Val::List(xs) => Val::Int(xs.len() as i64),
                Val::Vector(xs) => Val::Int(xs.len() as i64),
                Val::Str(s) => Val::Int(s.chars().count() as i64),
                Val::Map(m) => Val::Int(m.len() as i64),
                Val::Path { rels, .. } => Val::Int(rels.len() as i64),
                Val::Null => Val::Null,
                other => bail!(
                    "{n}() needs a collection or string, got {}",
                    other.to_display()
                ),
            },
            // nodes(path)/relationships(path): the path's node / relationship
            // sequence as a list (FalkorDB `AR_NODES`/`AR_RELATIONSHIPS`).
            "nodes" => match a0(0) {
                Val::Path { nodes, .. } => Val::List(nodes.into_iter().map(Val::Node).collect()),
                Val::Null => Val::Null,
                other => bail!("nodes() needs a path, got {}", other.to_display()),
            },
            "relationships" => match a0(0) {
                Val::Path { rels, .. } => Val::List(rels),
                Val::Null => Val::Null,
                other => bail!("relationships() needs a path, got {}", other.to_display()),
            },
            "head" => match a0(0) {
                Val::List(xs) => xs.into_iter().next().unwrap_or(Val::Null),
                Val::Null => Val::Null,
                other => bail!("head() needs a list, got {}", other.to_display()),
            },
            "last" => match a0(0) {
                Val::List(xs) => xs.into_iter().last().unwrap_or(Val::Null),
                Val::Null => Val::Null,
                other => bail!("last() needs a list, got {}", other.to_display()),
            },
            // `tostring`/`toString` and the `*OrNull` variant. FalkorDB's plain
            // `toString` errors on a non-convertible type while `toStringOrNull`
            // yields NULL; our renderer never errors, so the two coincide here.
            // `toString`/`toStringOrNull`: the renderer never errors, so the two
            // coincide. Kept here (not delegated) because a runtime-only argument
            // (node / temporal / point) must render via `Val::to_display`, not NULL.
            "tostring" | "tostringornull" => match a0(0) {
                Val::Null => Val::Null,
                v => match val_to_value(&v) {
                    Some(vv) => Val::from_value(
                        slater_scalar::eval_pure("tostring", &[vv])?.expect("handled"),
                    ),
                    None => Val::Str(v.to_display()),
                },
            },
            // left/right: the n leftmost/rightmost characters. n must be >= 0;
            // when the string is shorter than n the whole string is returned.
            "left" => self.left_right(&args, true)?,
            "right" => self.left_right(&args, false)?,
            // typeOf — FalkorDB's value-type name (SIType_ToString).
            "typeof" => Val::Str(type_name(&a0(0)).to_string()),
            // isEmpty — empty string / list / map. NULL argument → NULL.
            "isempty" => match a0(0) {
                Val::Str(s) => Val::Bool(s.is_empty()),
                Val::List(xs) => Val::Bool(xs.is_empty()),
                Val::Map(m) => Val::Bool(m.is_empty()),
                Val::Null => Val::Null,
                other => bail!(
                    "isEmpty() needs a string, list or map, got {}",
                    other.to_display()
                ),
            },
            "exists" => Val::Bool(!matches!(a0(0), Val::Null)),
            "substring" => self.substring(&args)?,
            "split" => match (a0(0), a0(1)) {
                (Val::Str(s), Val::Str(sep)) => {
                    Val::List(s.split(&sep).map(|p| Val::Str(p.to_string())).collect())
                }
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                _ => bail!("split() needs two strings"),
            },
            "replace" => match (a0(0), a0(1), a0(2)) {
                (Val::Str(s), Val::Str(a), Val::Str(b)) => Val::Str(s.replace(&a, &b)),
                (Val::Null, _, _) => Val::Null,
                _ => bail!("replace() needs three strings"),
            },
            "string.join" => string_join(&args)?,
            "string.matchregex" => self.match_regex(&a0(0), &a0(1))?,
            "string.replaceregex" => {
                let repl = if args.len() >= 3 {
                    a0(2)
                } else {
                    Val::Str(String::new())
                };
                self.replace_regex(&a0(0), &a0(1), &repl)?
            }
            "range" => self.range_fn(&args)?,
            "keys" => Val::List(
                self.all_properties(&a0(0))?
                    .into_iter()
                    .map(|(k, _)| Val::Str(k))
                    .collect(),
            ),
            "properties" => Val::Map(self.all_properties(&a0(0))?),
            "labels" => match a0(0) {
                Val::Node(id) => Val::List(
                    self.node_label_ids(id)?
                        .into_iter()
                        .filter_map(|l| self.gen.label_name(l).map(|s| Val::Str(s.to_string())))
                        .collect(),
                ),
                Val::Null => Val::Null,
                other => bail!("labels() needs a node, got {}", other.to_display()),
            },
            "id" => match a0(0) {
                Val::Node(id) | Val::Rel { id, .. } => Val::Int(id as i64),
                Val::Null => Val::Null,
                other => bail!(
                    "id() needs a node or relationship, got {}",
                    other.to_display()
                ),
            },
            "type" => match a0(0) {
                Val::Rel { reltype, .. } => self
                    .gen
                    .reltype_name(reltype)
                    .map(|s| Val::Str(s.to_string()))
                    .unwrap_or(Val::Null),
                Val::Null => Val::Null,
                other => bail!("type() needs a relationship, got {}", other.to_display()),
            },
            // startNode/endNode return the stored-direction endpoints carried on
            // the relationship value (src→dst), so no re-traversal is needed. Match
            // FalkorDB: NULL argument → NULL; a non-relationship is an error.
            "startnode" => match a0(0) {
                Val::Rel { start, .. } => Val::Node(start),
                Val::Null => Val::Null,
                other => bail!(
                    "startNode() needs a relationship, got {}",
                    other.to_display()
                ),
            },
            "endnode" => match a0(0) {
                Val::Rel { end, .. } => Val::Node(end),
                Val::Null => Val::Null,
                other => bail!("endNode() needs a relationship, got {}", other.to_display()),
            },
            // Build a first-class vector from a list of numbers — the inlined
            // `vecf32([...])` form drivers send (and the query-vector argument of
            // db.idx.vector.queryNodes). Round-trips a `Vector` unchanged.
            "vecf32" => match a0(0) {
                Val::Vector(v) => Val::Vector(v),
                Val::List(xs) => Val::Vector(
                    xs.iter()
                        .enumerate()
                        .map(|(i, x)| embed_component(i, x, "vecf32()"))
                        .collect::<Result<_>>()?,
                ),
                Val::Null => Val::Null,
                other => bail!(
                    "vecf32() needs a list of numbers, got {}",
                    other.to_display()
                ),
            },
            // Cosine similarity of two vectors, in [-1, 1] (higher = more similar).
            // Complements the KNN `score`, which is the distance `1 - similarity`
            // (D26). Accepts vectors or numeric lists.
            "similarity" | "vec.cosinesimilarity" => match (as_vector(&a0(0))?, as_vector(&a0(1))?)
            {
                (Some(a), Some(b)) if a.len() == b.len() => {
                    Val::Float(vector::cosine_similarity(&a, &b))
                }
                (Some(a), Some(b)) => bail!(
                    "similarity() needs equal-length vectors ({} vs {})",
                    a.len(),
                    b.len()
                ),
                _ => Val::Null,
            },
            // Euclidean / cosine *distance* between two vectors (FalkorDB
            // vector_funcs.c). NULL operand → NULL; a dimension mismatch or a
            // non-vector operand is an error, matching FalkorDB's "Vector
            // dimension mismatch" / type-mismatch behaviour. Accept vectors or
            // numeric lists, like `similarity`.
            "vec.euclideandistance" | "vec.cosinedistance" => {
                let (x, y) = (a0(0), a0(1));
                if matches!(x, Val::Null) || matches!(y, Val::Null) {
                    Val::Null
                } else {
                    let a = as_vector(&x)?.ok_or_else(|| {
                        anyhow::anyhow!("{n}() needs vectors, got {}", x.to_display())
                    })?;
                    let b = as_vector(&y)?.ok_or_else(|| {
                        anyhow::anyhow!("{n}() needs vectors, got {}", y.to_display())
                    })?;
                    if a.len() != b.len() {
                        bail!("Vector dimension mismatch, {} != {}", a.len(), b.len());
                    }
                    if n == "vec.euclideandistance" {
                        Val::Float(vector::euclidean_distance(&a, &b))
                    } else {
                        Val::Float(vector::cosine_distance(&a, &b))
                    }
                }
            }
            // ── Point / geo functions (FalkorDB point_funcs.c) ──────────────
            // point({latitude, longitude}): a WGS-84 geographic point. FalkorDB
            // accepts ONLY the lat/lon map form (no Cartesian x/y, no SRID arg);
            // the map must have exactly those two numeric keys, latitude in
            // [-90,90] and longitude in [-180,180]. NULL map → NULL.
            "point" => match a0(0) {
                Val::Null => Val::Null,
                Val::Map(m) => {
                    if m.len() != 2 {
                        bail!("A point map should have 2 elements, latitude and longitude");
                    }
                    let get = |k: &str| m.iter().find(|(key, _)| key == k).map(|(_, v)| v);
                    let lat = get("latitude").ok_or_else(|| {
                        anyhow::anyhow!("Did not find 'latitude' value in point map")
                    })?;
                    let lon = get("longitude").ok_or_else(|| {
                        anyhow::anyhow!("Did not find 'longitude' value in point map")
                    })?;
                    let (latitude, longitude) = match (lat.as_num(), lon.as_num()) {
                        (Some(a), Some(b)) => (a, b),
                        _ => bail!(
                            "'latitude' and 'longitude' values in point map were not both valid numerics"
                        ),
                    };
                    if !(-90.0..=90.0).contains(&latitude) {
                        bail!("latitude should be within the -90 to 90 range");
                    }
                    if !(-180.0..=180.0).contains(&longitude) {
                        bail!("longitude should be within the -180 to 180 range");
                    }
                    Val::Point {
                        latitude,
                        longitude,
                    }
                }
                other => bail!("point() expects a map, got {}", other.to_display()),
            },
            // distance(p1, p2): great-circle distance in metres (haversine over the
            // WGS-84 sphere, FalkorDB `AR_DISTANCE`). NULL operand → NULL.
            "distance" => match (a0(0), a0(1)) {
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                (
                    Val::Point {
                        latitude: la,
                        longitude: lo_a,
                    },
                    Val::Point {
                        latitude: lb,
                        longitude: lo_b,
                    },
                ) => Val::Float(haversine_metres(la, lo_a, lb, lo_b)),
                (a, b) => bail!(
                    "distance() needs two points, got {} and {}",
                    a.to_display(),
                    b.to_display()
                ),
            },
            // ── Temporal constructors (FalkorDB time_funcs.c) ───────────────
            // Each takes a string (ISO-8601) or a component map; a bad string →
            // NULL, NULL arg → NULL. A no-arg call would be the wall-clock `now`,
            // which is out of scope (non-deterministic) → NULL. `timestamp()`
            // shipped in Phase 1 as an Int and is unchanged.
            "date" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::date_from_string(&s)
                    .map(Val::Date)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_date(&m)?,
                other => bail!("date() expects a string or map, got {}", other.to_display()),
            },
            "localtime" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::time_from_string(&s)
                    .map(Val::Time)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_time(&m)?,
                other => bail!(
                    "localtime() expects a string or map, got {}",
                    other.to_display()
                ),
            },
            "localdatetime" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::datetime_from_string(&s)
                    .map(Val::DateTime)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_datetime(&m)?,
                other => bail!(
                    "localdatetime() expects a string or map, got {}",
                    other.to_display()
                ),
            },
            "duration" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::duration_from_string(&s)?
                    .map(Val::Duration)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_duration(&m)?,
                other => bail!(
                    "duration() expects a string or map, got {}",
                    other.to_display()
                ),
            },
            // ── Non-deterministic builtins (wall-clock / RNG) ────────────────
            // These read the clock or an entropy source, so `parser::is_nondeterministic`
            // marks any query calling them non-cacheable (server.rs `run_query`
            // skips the result-cache get + insert) — otherwise a cache hit would
            // replay a stale value.
            // `rand()` → uniform double in [0,1) (FalkorDB `AR_RAND`: rand()/RAND_MAX).
            "rand" => Val::Float(random_f64()),
            // `randomUUID()` → a fresh RFC-4122 v4 UUID string (FalkorDB `AR_RANDOMUUID`).
            "randomuuid" => Val::Str(uuid::Uuid::new_v4().to_string()),
            // `timestamp()` → milliseconds since the Unix epoch (FalkorDB `AR_TIMESTAMP`).
            "timestamp" => Val::Int(now_millis()),
            // ── List functions (FalkorDB list_funcs.c) ──────────────────────
            // tail: all but the first element. NULL → NULL.
            "tail" => match a0(0) {
                Val::Null => Val::Null,
                Val::List(xs) => Val::List(xs.into_iter().skip(1).collect()),
                other => bail!("tail() needs a list, got {}", other.to_display()),
            },
            // list.dedup: drop later duplicates, preserving first-seen order.
            "list.dedup" => match a0(0) {
                Val::Null => Val::Null,
                Val::List(mut xs) => {
                    dedup_vals(&mut xs);
                    Val::List(xs)
                }
                other => bail!("list.dedup() needs a list, got {}", other.to_display()),
            },
            // list.sort(list, ascending = true): sorted copy by total order.
            "list.sort" => list_sort(&args)?,
            // list.remove(list, idx, count = 1): drop up-to-`count` elements.
            "list.remove" => list_remove(&args)?,
            // list.insert(list, idx, val, dups = true): insert one element.
            "list.insert" => list_insert(&args)?,
            // list.insertListElements(list, list2, idx, dups = true): splice a list.
            "list.insertlistelements" => list_insert_elements(&args)?,
            // Element-wise conversion lists; each element goes through the
            // matching `*OrNull` scalar (NULL on failure). NULL list → NULL.
            "tobooleanlist" => self.to_type_list(&a0(0), "toboolean")?,
            "tofloatlist" => self.to_type_list(&a0(0), "tofloat")?,
            "tointegerlist" => self.to_type_list(&a0(0), "tointeger")?,
            "tostringlist" => self.to_type_list(&a0(0), "tostring")?,
            // ── Entity functions (FalkorDB entity_funcs.c) ──────────────────
            // hasLabels(node, [labels]): node carries ALL given labels. The
            // operator form `n:Label` is handled separately by `Expr::HasLabels`.
            "haslabels" => match a0(0) {
                Val::Null => Val::Null,
                Val::Node(id) => {
                    let labels = match a0(1) {
                        Val::List(xs) => xs,
                        Val::Null => return Ok(Val::Null),
                        other => bail!(
                            "hasLabels() needs a list of label strings, got {}",
                            other.to_display()
                        ),
                    };
                    let have = self.node_label_ids(id)?;
                    let mut res = true;
                    for l in labels {
                        let name = match l {
                            Val::Str(s) => s,
                            other => bail!(
                                "hasLabels() labels must be strings, got {}",
                                other.to_display()
                            ),
                        };
                        match self.gen.label_id(&name) {
                            Some(lid) if have.contains(&lid) => {}
                            _ => {
                                res = false;
                                break;
                            }
                        }
                    }
                    Val::Bool(res)
                }
                other => bail!("hasLabels() needs a node, got {}", other.to_display()),
            },
            // indegree/outdegree(node, [types…]): count edges in one direction,
            // optionally restricted to the given relationship type(s) (passed as
            // varargs strings or a single array of strings).
            "indegree" => self.node_degree(&args, true)?,
            "outdegree" => self.node_degree(&args, false)?,
            other => bail!("unknown function '{other}'"),
        })
    }

    /// Element-wise list conversion shared by `to{Boolean,Float,Integer,String}List`:
    /// run each element through the named `*OrNull`-style scalar arm. A NULL list
    /// yields NULL (FalkorDB `_AR_TOTYPELIST`).
    pub(crate) fn to_type_list(&self, v: &Val, conv: &str) -> Result<Val> {
        match v {
            Val::Null => Ok(Val::Null),
            Val::List(xs) => {
                let mut out = Vec::with_capacity(xs.len());
                for x in xs {
                    out.push(self.call_function(conv, false, vec![x.clone()])?);
                }
                Ok(Val::List(out))
            }
            other => bail!("{conv}List() needs a list, got {}", other.to_display()),
        }
    }

    /// `indegree`/`outdegree`: count a node's edges in one direction, optionally
    /// filtered to specific relationship types. Mirrors FalkorDB `_AR_NodeDegree`:
    /// a NULL node yields NULL; type filters may be varargs strings or one array.
    pub(crate) fn node_degree(&self, args: &[Val], incoming: bool) -> Result<Val> {
        let dir = if incoming { "indegree" } else { "outdegree" };
        let id = match args.first() {
            Some(Val::Node(id)) => *id,
            Some(Val::Null) | None => return Ok(Val::Null),
            Some(other) => bail!("{dir}() needs a node, got {}", other.to_display()),
        };
        // Collect the (deduplicated) relationship-type filter, if any.
        let mut names: Vec<String> = Vec::new();
        if args.len() > 1 {
            let push = |names: &mut Vec<String>, v: &Val| -> Result<()> {
                match v {
                    Val::Str(s) => {
                        if !names.contains(s) {
                            names.push(s.clone());
                        }
                        Ok(())
                    }
                    other => bail!("{dir}() types must be strings, got {}", other.to_display()),
                }
            };
            match &args[1] {
                Val::List(xs) => {
                    for x in xs {
                        push(&mut names, x)?;
                    }
                }
                _ => {
                    for a in &args[1..] {
                        push(&mut names, a)?;
                    }
                }
            }
        }
        let adjs = if incoming {
            self.incoming(id)?
        } else {
            self.outgoing(id)?
        };
        let count = if args.len() > 1 {
            let type_ids: Vec<u32> = names
                .iter()
                .filter_map(|t| self.gen.reltype_id(t))
                .collect();
            adjs.iter()
                .filter(|a| type_ids.contains(&a.reltype))
                .count()
        } else {
            adjs.len()
        };
        Ok(Val::Int(count as i64))
    }

    pub(crate) fn substring(&self, args: &[Val]) -> Result<Val> {
        let s = match args.first() {
            Some(Val::Str(s)) => s,
            Some(Val::Null) | None => return Ok(Val::Null),
            Some(other) => bail!("substring() needs a string, got {}", other.to_display()),
        };
        let chars: Vec<char> = s.chars().collect();
        let start = match args.get(1) {
            Some(Val::Int(i)) if *i >= 0 => (*i as usize).min(chars.len()),
            _ => bail!("substring() start must be a non-negative integer"),
        };
        let end = match args.get(2) {
            Some(Val::Int(len)) if *len >= 0 => (start + *len as usize).min(chars.len()),
            None => chars.len(),
            _ => bail!("substring() length must be a non-negative integer"),
        };
        Ok(Val::Str(chars[start..end].iter().collect()))
    }

    /// `left(s, n)` / `right(s, n)`: the n leftmost (or rightmost) characters.
    /// NULL string → NULL; `n` must be a non-negative integer; an `n` past the
    /// string length returns the whole string (matching FalkorDB AR_LEFT/AR_RIGHT).
    pub(crate) fn left_right(&self, args: &[Val], from_left: bool) -> Result<Val> {
        let s = match args.first() {
            Some(Val::Str(s)) => s,
            Some(Val::Null) | None => return Ok(Val::Null),
            Some(other) => bail!(
                "{}() needs a string, got {}",
                if from_left { "left" } else { "right" },
                other.to_display()
            ),
        };
        let n = match args.get(1) {
            Some(Val::Int(i)) if *i >= 0 => *i as usize,
            _ => bail!("length must be a non-negative integer"),
        };
        let chars: Vec<char> = s.chars().collect();
        if n >= chars.len() {
            return Ok(Val::Str(s.clone()));
        }
        let slice = if from_left {
            &chars[..n]
        } else {
            &chars[chars.len() - n..]
        };
        Ok(Val::Str(slice.iter().collect()))
    }

    pub(crate) fn range_fn(&self, args: &[Val]) -> Result<Val> {
        let int = |v: Option<&Val>, d: i64| match v {
            Some(Val::Int(i)) => Ok(*i),
            None => Ok(d),
            _ => bail!("range() bounds must be integers"),
        };
        let start = int(args.first(), 0)?;
        let end = int(args.get(1), 0)?;
        let step = int(args.get(2), 1)?;
        if step == 0 {
            bail!("range() step must be non-zero");
        }
        // Bound the result *before* allocating. A naive loop over `range(0,
        // i64::MAX)` allocates until it OOMs, and the unchecked `i += step` it used
        // to do wraps on overflow into an infinite loop (the per-query deadline is
        // not consulted inside this tight loop). Compute the element count in i128
        // (so `end - start` cannot overflow) and refuse anything past the guardrail.
        // 1M × ~48 B/element ≈ 48 MB — the lone guard when `query.maxIntermediate`
        // is disabled, so it is sized for the 100–200 MB deployment envelope.
        const MAX_RANGE_LEN: i128 = 1_000_000;
        let count: i128 = {
            let (s, e, st) = (start as i128, end as i128, step as i128);
            if (st > 0 && s > e) || (st < 0 && s < e) {
                0
            } else {
                (e - s) / st + 1
            }
        };
        if count > MAX_RANGE_LEN {
            bail!("range() would produce {count} elements, exceeding the limit of {MAX_RANGE_LEN}");
        }
        // Also charge the query-wide budget before allocating, so repeated
        // near-limit ranges cannot stack up to unbounded memory.
        self.charge(count as u64)?;
        let mut out = Vec::with_capacity(count as usize);
        let mut i = start;
        // Inclusive of `end`, matching Cypher.
        while (step > 0 && i <= end) || (step < 0 && i >= end) {
            out.push(Val::Int(i));
            // Stop cleanly at the i64 boundary instead of wrapping into an infinite
            // loop (the count guard above already bounds the iteration otherwise).
            match i.checked_add(step) {
                Some(n) => i = n,
                None => break,
            }
        }
        Ok(Val::List(out))
    }
}

impl<'g, V: ReadView> Engine<'g, V> {
    /// Compile `pattern`, or fetch it from the per-query cache. `anchored` wraps
    /// it as `\A(?:…)\z` so `=~` requires the entire subject to match —
    /// openCypher / FalkorDB `=~` semantics; the unanchored form scans for every
    /// non-overlapping match anywhere in the subject.
    pub(crate) fn compiled_regex(&self, pattern: &str, anchored: bool) -> Result<regex::Regex> {
        if pattern.len() > MAX_REGEX_PATTERN_BYTES {
            bail!(
                "regex pattern is {} bytes, exceeding the limit of {MAX_REGEX_PATTERN_BYTES}",
                pattern.len()
            );
        }
        let key = if anchored {
            format!(r"\A(?:{pattern})\z")
        } else {
            pattern.to_string()
        };
        if let Some(re) = self.regex_cache.borrow().get(&key) {
            return Ok(re.clone());
        }
        let re = regex::RegexBuilder::new(&key)
            .size_limit(REGEX_SIZE_LIMIT)
            .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid regex: {e}"))?;
        let mut cache = self.regex_cache.borrow_mut();
        if cache.len() < REGEX_CACHE_MAX {
            cache.insert(key, re.clone());
        }
        Ok(re)
    }

    pub(crate) fn string_op(&self, op: StrOp, a: &Val, b: &Val) -> Result<Val> {
        let (s, t) = match (a, b) {
            (Val::Str(s), Val::Str(t)) => (s, t),
            // `=~` against a null operand is null (three-valued); so are the others.
            _ => return Ok(Val::Null),
        };
        Ok(Val::Bool(match op {
            StrOp::StartsWith => s.starts_with(t.as_str()),
            StrOp::EndsWith => s.ends_with(t.as_str()),
            StrOp::Contains => s.contains(t.as_str()),
            // `=~` is a full-match: the whole string must match the pattern,
            // mirroring FalkorDB's `str_MatchRegex` (anchored at both ends).
            StrOp::Regex => self.compiled_regex(t, true)?.is_match(s),
        }))
    }

    // string.matchRegEx(str, regex) -> list of [full_match, group1, …] per match.
    // A null operand yields an empty list; non-participating groups become "".
    pub(crate) fn match_regex(&self, s: &Val, pat: &Val) -> Result<Val> {
        let (s, pat) = match (s, pat) {
            (Val::Str(s), Val::Str(p)) => (s, p),
            (Val::Null, _) | (_, Val::Null) => return Ok(Val::List(vec![])),
            (Val::Str(_), other) | (other, _) => bail!(
                "Type mismatch: expected String or Null but was {}",
                type_name(other)
            ),
        };
        let re = self.compiled_regex(pat, false)?;
        let mut out = Vec::new();
        for caps in re.captures_iter(s) {
            let row = caps
                .iter()
                .map(|g| Val::Str(g.map_or("", |m| m.as_str()).to_string()))
                .collect();
            out.push(Val::List(row));
        }
        Ok(Val::List(out))
    }

    // string.replaceRegEx(str, regex, replacement = '') -> string. Any null operand
    // yields null; the replacement is inserted literally (no `$group` expansion).
    pub(crate) fn replace_regex(&self, s: &Val, pat: &Val, repl: &Val) -> Result<Val> {
        let (s, pat, repl) = match (s, pat, repl) {
            (Val::Str(s), Val::Str(p), Val::Str(r)) => (s, p, r),
            (Val::Null, _, _) | (_, Val::Null, _) | (_, _, Val::Null) => return Ok(Val::Null),
            (Val::Str(_), Val::Str(_), other) | (Val::Str(_), other, _) | (other, _, _) => bail!(
                "Type mismatch: expected String or Null but was {}",
                type_name(other)
            ),
        };
        let re = self.compiled_regex(pat, false)?;
        Ok(Val::Str(
            re.replace_all(s, regex::NoExpand(repl)).into_owned(),
        ))
    }
}
