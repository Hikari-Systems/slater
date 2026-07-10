// SPDX-License-Identifier: Apache-2.0
//! Read-only Cypher parser: source text → typed AST.
//!
//! The grammar (`cypher.pest`) is the online query language — separate from
//! `slater-build`'s dump dialect. This module drives it and lowers the parse tree
//! into the [`ast`] types the planner/executor consume. Write and procedure
//! clauses parse structurally but are rejected here with a clear "Slater is
//! read-only" error, so a client gets a meaningful `FAILURE` rather than an opaque
//! syntax error.
//
// The planner/executor that consume this AST land in the next M4.5 increment;
// allow dead_code for the AST + parser until then.
#![allow(dead_code)]

use anyhow::{bail, Result};
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use graph_format::ids::Value;

#[derive(Parser)]
#[grammar = "cypher.pest"]
struct CypherParser;

/// Production-by-production conformance of `cypher.pest` against the openCypher
/// reference grammar. Kept in its own file: it is a specification, not a unit test.
#[cfg(test)]
#[path = "parser_conformance_tests.rs"]
mod conformance;

pub mod ast {
    //! The read-Cypher abstract syntax tree.
    use graph_format::ids::Value;

    /// A full query: a head part plus zero or more `UNION[ ALL]`-joined parts.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Query {
        pub head: SingleQuery,
        /// Each tail entry is `(union_all, part)` where `union_all` distinguishes
        /// `UNION ALL` (true) from `UNION` (false, distinct).
        pub tail: Vec<(bool, SingleQuery)>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SingleQuery {
        pub reading: Vec<Clause>,
        pub ret: ReturnClause,
    }

    /// The single durable write shape the writable layer accepts:
    /// `(MATCH|MERGE) (var:Label {key: <literal|param>})
    ///  (SET var.prop = <literal|param>[, …] | [DETACH] DELETE var) [RETURN …]`.
    /// It anchors one node by a business key and mutates it. The key value and every
    /// SET right-hand side are held as [`Expr`] (a literal or a parameter) so
    /// parameters resolve at execution time, when their values are known.
    #[derive(Debug, Clone, PartialEq)]
    pub struct WriteStmt {
        /// The anchor variable (a SET assignment must target it; DELETE names it).
        pub var: String,
        /// The single anchor label — the business key's label half.
        pub label: String,
        /// The inline business-key property name.
        pub key: String,
        /// The inline business-key value (a literal or a parameter).
        pub key_value: Expr,
        /// Anchor create-semantics (Phase 2c): `true` for `MERGE` (create the node if
        /// the business key is absent, else patch it); `false` for `MATCH` (address an
        /// existing node only — an absent key is an error). Always `false` for DELETE.
        pub upsert: bool,
        /// The unconditional mutation applied to the anchored node (an empty `Set` when a
        /// MERGE carries only `ON CREATE`/`ON MATCH` blocks).
        pub op: WriteOp,
        /// `MERGE … ON CREATE SET …` — items applied only when the MERGE **creates** the
        /// node. Empty unless present; only valid on a MERGE (rejected on MATCH).
        pub on_create: Vec<SetItem>,
        /// `MERGE … ON MATCH SET …` — items applied only when the MERGE **matches** an
        /// existing node. Empty unless present; only valid on a MERGE.
        pub on_match: Vec<SetItem>,
        /// An optional `RETURN` projecting the (post-write) anchor node.
        pub ret: Option<ReturnClause>,
        /// A leading `UNWIND <list> AS <var>` (write-UNWIND): `Some((list_expr, var))`
        /// makes this a **batched** write — one write per list element under a single
        /// group commit, with `key_value`/SET values evaluated per row (they may
        /// reference `<var>`'s fields). `None` is a plain single write, whose values
        /// must be constants (a literal or parameter).
        pub unwind: Option<(Expr, String)>,
    }

    /// The mutation a [`WriteStmt`] applies to its business-key-anchored node.
    #[derive(Debug, Clone, PartialEq)]
    pub enum WriteOp {
        /// `SET` items in source order (later wins). Each targets the anchor variable.
        Set(Vec<SetItem>),
        /// `REMOVE` items in source order — property drops and label drops.
        Remove(Vec<RemoveItem>),
        /// Tombstone the node(s). `detach` records a `DETACH DELETE` (also removes
        /// incident edges). `targets` are the named variables to delete; in the
        /// anchored write model every target is the anchor variable.
        Delete { detach: bool, targets: Vec<String> },
    }

    /// One `SET` assignment on the anchor node. openCypher's four item shapes.
    #[derive(Debug, Clone, PartialEq)]
    pub enum SetItem {
        /// `SET var.prop = value` — write one property (value a literal or parameter,
        /// or a row-field reference under a write-`UNWIND`).
        Prop { prop: String, value: Expr },
        /// `SET var = {map}` — replace *all* of the node's properties with the map.
        ReplaceMap(Vec<(String, Expr)>),
        /// `SET var += {map}` — merge the map into the node's existing properties.
        MergeMap(Vec<(String, Expr)>),
        /// `SET var:Label[:Label…]` — add labels to the node.
        AddLabels(Vec<String>),
    }

    /// One `REMOVE` item on the anchor node.
    #[derive(Debug, Clone, PartialEq)]
    pub enum RemoveItem {
        /// `REMOVE var.prop` — drop a property.
        Prop(String),
        /// `REMOVE var:Label[:Label…]` — drop labels.
        Labels(Vec<String>),
    }

    /// One endpoint of a relationship write: a single-label node addressed by one
    /// inline business-key property (a literal or parameter). The reduced form of a
    /// [`WriteStmt`] anchor — an edge write has two of them and no SET.
    #[derive(Debug, Clone, PartialEq)]
    pub struct EndpointPat {
        pub label: String,
        pub key: String,
        pub key_value: Expr,
    }

    /// A writable-layer relationship write: create or delete the edge
    /// `(src) -[reltype]-> (dst)`, each endpoint anchored by a business key (Phase 3c).
    #[derive(Debug, Clone, PartialEq)]
    pub struct EdgeWriteStmt {
        pub src: EndpointPat,
        pub reltype: String,
        pub dst: EndpointPat,
        pub op: EdgeWriteOp,
        /// Relationship property assignments from an optional `SET r.p = …` on a
        /// `MERGE` (each value a literal or parameter). Empty for a bare `MERGE` or a
        /// `DELETE`. Only a **delta-born** edge carries editable properties for now.
        pub sets: Vec<(String, Expr)>,
    }

    /// The mutation an [`EdgeWriteStmt`] applies.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum EdgeWriteOp {
        /// `MERGE (a)-[:R]->(b)` — create the edge if absent (auto-creating an absent
        /// endpoint node), else a no-op (idempotent by edge identity).
        Create,
        /// `MATCH (a)-[r:R]->(b) DELETE r` — tombstone the edge.
        Delete,
    }

    /// `CREATE (n:Label {props})` — create a node unconditionally (Stage 7). Unlike a
    /// MATCH/MERGE anchor there is no single inline business key: every property is inline,
    /// and the server designates the range-indexed one as the business key.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CreateStmt {
        pub var: String,
        pub label: String,
        /// All inline properties (the business key is identified by the server).
        pub props: Vec<(String, Expr)>,
        pub ret: Option<ReturnClause>,
    }

    /// A parsed statement: a read query, or a writable-layer write. The server
    /// dispatches on this — see [`super::parse_statement`].
    // An AST enum: variant sizes vary (a full read `Query` vs a small write). Boxing every
    // variant to equalise them is noise for a short-lived parse result.
    #[allow(clippy::large_enum_variant)]
    #[derive(Debug, Clone, PartialEq)]
    pub enum Statement {
        Read(Query),
        Write(WriteStmt),
        Create(CreateStmt),
        WriteEdge(EdgeWriteStmt),
        /// `CALL slater.consolidate()` — fold the writable delta into a fresh
        /// generation and swap it in (Phase 5). Takes no arguments and targets the
        /// session's current graph; the server dispatches it to
        /// `Graphs::consolidate_graph`.
        Consolidate,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Clause {
        Match(MatchClause),
        With(WithClause),
        VectorCall(VectorCallClause),
        Call(CallClause),
        CallSubquery(CallSubqueryClause),
        Unwind(UnwindClause),
    }

    /// Which outer-scope variables a `CALL { … }` subquery branch imports, taken
    /// from its leading `WITH`. Only "simple" imports are allowed (FalkorDB
    /// `_ValidateCallInitialWith`): bare variable references, no aliasing, and no
    /// `WHERE`/`ORDER BY`/`SKIP`/`LIMIT`.
    #[derive(Debug, Clone, PartialEq)]
    pub enum Imports {
        /// No leading `WITH` — the subquery sees none of the outer variables.
        None,
        /// `WITH *` — every outer variable is imported.
        All,
        /// `WITH a, b, …` — exactly the named outer variables are imported.
        Named(Vec<String>),
    }

    /// A `CALL { <subquery> }` clause (Phase 12). The inner query runs once per
    /// outer row with its imported variables seeded; a returning subquery
    /// multiplies the outer cardinality by its result rows, while a unit
    /// (`RETURN`-less) subquery passes the outer rows through unchanged.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CallSubqueryClause {
        /// The inner read query (head plus any `UNION`-joined parts). Leading
        /// import `WITH`s are kept in place; they re-project the seeded imports.
        pub inner: Box<Query>,
        /// What each branch imports, in branch order (head first, then each
        /// `UNION` part). `imports.len() == 1 + inner.tail.len()`.
        pub imports: Vec<Imports>,
        /// Whether the subquery returns rows (multiplies cardinality) or is a unit
        /// subquery (passes the outer rows through). All branches agree.
        pub returning: bool,
    }

    /// A read-only metadata procedure call (Phase 11): `CALL db.meta.stats()`,
    /// `CALL dbms.procedures() YIELD name, mode`, etc. The procedure takes no
    /// arguments; its named outputs are bound by `YIELD` (or all of them, when
    /// `YIELD` is absent) and an optional `WHERE` filters the yielded rows.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CallClause {
        /// The procedure name as written (case preserved); dispatch lowercases it.
        pub name: String,
        /// Call arguments (none of these procedures take any — kept for the parse
        /// shape and a clear "takes no arguments" error at exec).
        pub args: Vec<Expr>,
        /// `(procedure output, bound variable)` pairs from `YIELD`; empty means a
        /// bare call binding every output under its own name.
        pub yields: Vec<(String, String)>,
        /// An optional `WHERE` filtering the yielded rows.
        pub where_: Option<Expr>,
    }

    /// `UNWIND <expr> AS <var>` — a read clause that emits one row per element of
    /// the list `expr` evaluates to, binding the element to `var`.
    #[derive(Debug, Clone, PartialEq)]
    pub struct UnwindClause {
        pub expr: Expr,
        pub var: String,
    }

    /// The one permitted procedure call: `CALL db.idx.vector.queryNodes('Label',
    /// 'prop', k, queryVec) YIELD node, score`. Binds its YIELD outputs into scope
    /// like a `MATCH` introduces pattern variables.
    #[derive(Debug, Clone, PartialEq)]
    pub struct VectorCallClause {
        /// Node label the index ranges over (arg 0, a string literal).
        pub label: String,
        /// Indexed property (arg 1, a string literal).
        pub property: String,
        /// Number of neighbours to return (arg 2; literal or `$param`).
        pub k: Expr,
        /// The query vector (arg 3; a `vecf32([...])` literal or `$param`).
        pub query_vec: Expr,
        /// `(procedure output, bound variable)` pairs from `YIELD`. The outputs
        /// are FalkorDB's `node` and `score`; the bound name is the `AS` alias if
        /// present, else the output name.
        pub yields: Vec<(String, String)>,
        /// An optional `WHERE` filtering the yielded rows (FalkorDB's
        /// `YIELD ... WHERE ...`).
        pub where_: Option<Expr>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct MatchClause {
        pub optional: bool,
        pub patterns: Vec<Pattern>,
        pub where_: Option<Expr>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct WithClause {
        pub distinct: bool,
        pub body: ProjectionBody,
        pub where_: Option<Expr>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ReturnClause {
        pub distinct: bool,
        pub body: ProjectionBody,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ProjectionBody {
        /// `true` when the items begin with `*` (project all in-scope variables).
        pub star: bool,
        pub items: Vec<ProjItem>,
        pub order_by: Vec<(Expr, SortDir)>,
        pub skip: Option<Expr>,
        pub limit: Option<Expr>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ProjItem {
        pub expr: Expr,
        pub alias: Option<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SortDir {
        Asc,
        Desc,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Pattern {
        pub path_var: Option<String>,
        pub start: NodePat,
        pub rels: Vec<(RelPat, NodePat)>,
        /// `None` for an ordinary pattern (the whole chain lives in `rels`). `Some`
        /// only when the pattern contains a GQL quantified path group
        /// (`((…)){m,n}`); then `rels` is empty and the ordered element sequence —
        /// plain hops interleaved with quantified groups — lives here. The executor
        /// desugars these into ordinary (`segments: None`) patterns before matching
        /// (`expand_quantified_pattern`), so every consumer of `rels` is unaffected
        /// when `segments` is `None`.
        pub segments: Option<Vec<Segment>>,
        /// GQL path restrictor (`WALK`/`TRAIL`/`ACYCLIC`/`SIMPLE`), `None` when the
        /// pattern carries no explicit restrictor. `None` preserves slater's
        /// historical variable-length semantics (edge-unique = TRAIL), so existing
        /// queries are untouched; only `Walk` relaxes it and `Acyclic`/`Simple` add
        /// node-uniqueness. Scoped (PR 2) to variable-length relationships — see
        /// `expand_chain`/`varlen` in exec.rs.
        pub restrictor: Option<PathRestrictor>,
        /// GQL shortest-path selector (`ANY SHORTEST`/`ALL SHORTEST`/`SHORTEST k`),
        /// `None` when the pattern carries no selector. When `Some`, the executor
        /// drives a shortest-path search between the pattern's two endpoints rather
        /// than the ordinary matcher (`apply_match_selected` in exec.rs). Scoped
        /// (PR 3) to a single-relationship pattern, like `shortestPath()`.
        pub selector: Option<PathSelector>,
    }

    /// GQL shortest-path selector on a [`Pattern`]. Picks shortest connecting paths
    /// between the pattern's endpoints: `AnyShortest` yields one shortest path per
    /// endpoint pair; `AllShortest` yields every path of the minimum length;
    /// `ShortestK(k)` yields up to `k` paths in non-decreasing length order. Paths are
    /// loopless (no repeated node), mirroring `shortestPath()`'s simple-path search.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PathSelector {
        AnyShortest,
        AllShortest,
        ShortestK(u32),
    }

    /// GQL path restrictor controlling node/edge reuse over a variable-length walk.
    /// On a [`Pattern`] this is `Some` only when the query spells the restrictor out;
    /// the executor maps the *absence* of one onto today's edge-unique behaviour, so
    /// `Trail` and a bare `*` are identical and only `Walk` relaxes uniqueness.
    /// `Acyclic` forbids any repeated node; `Simple` forbids repeats too but lets the
    /// walk's two endpoints coincide (a single closed cycle).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PathRestrictor {
        Walk,
        Trail,
        Acyclic,
        Simple,
    }

    /// One element of a quantified [`Pattern`], in chain order. A `Hop` is an
    /// ordinary relationship + its end node; a `Quantified` group is an inner
    /// relationship sub-chain repeated `bounds` times, terminating at `exit` (the
    /// node written after the group's closing `)`).
    #[derive(Debug, Clone, PartialEq)]
    pub enum Segment {
        Hop(RelPat, NodePat),
        Quantified {
            /// The inner sub-path's relationship chain (excluding its leading node,
            /// which juxtaposes with the preceding element's node).
            inner: Vec<(RelPat, NodePat)>,
            bounds: VarLength,
            /// The node following the group; the last node of the last repetition
            /// unifies with it.
            exit: NodePat,
        },
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct NodePat {
        pub var: Option<String>,
        /// `None` ≡ no label constraint (matches any node). `Some` carries the GQL
        /// label boolean expression; classic `:A` / `:A:B` lower to `Atom` / `And`.
        pub label_expr: Option<LabelExpr>,
        pub props: Vec<(String, Expr)>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct RelPat {
        pub var: Option<String>,
        pub dir: Direction,
        /// `None` ≡ any relationship type. `Some` carries the same `LabelExpr` AST as
        /// node labels (reused, not a parallel type) — a relationship has exactly one
        /// type, so the expression is evaluated over that singleton (`:T1|T2` matches
        /// either, `:A&B` can never hold, `:!T` excludes one type).
        pub type_expr: Option<LabelExpr>,
        pub var_length: Option<VarLength>,
        pub props: Vec<(String, Expr)>,
    }

    /// A GQL label / relationship-type boolean expression: `!` (highest) > `&` > `|`,
    /// with parentheses. Evaluated as plain set membership over a node's label set or
    /// a relationship's single type — three-valued logic is not needed because a
    /// label is simply present or absent.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum LabelExpr {
        Atom(String),
        And(Box<LabelExpr>, Box<LabelExpr>),
        Or(Box<LabelExpr>, Box<LabelExpr>),
        Not(Box<LabelExpr>),
    }

    impl LabelExpr {
        /// `Some(label)` iff this expression is exactly one positive atom. The planner
        /// and the single-node fast paths use this to keep the common `(:Person)` case
        /// on the cheap LabelScan / single-posting path; anything richer falls back to
        /// a broader scan plus a full `node_ok` re-check.
        pub fn as_single_atom(&self) -> Option<&String> {
            match self {
                LabelExpr::Atom(a) => Some(a),
                _ => None,
            }
        }

        /// If the expression is a single atom or an `OR`-tree of atoms (no `&`/`!`),
        /// return that flat list of type/label names; otherwise `None`. Relationship
        /// traversal uses this to keep the pre-GQL "any of these reltypes" id-set hot
        /// loop for single-type and `:T1|T2` patterns, only evaluating a general
        /// boolean expression per edge when `&`/`!` actually appear.
        pub fn positive_atoms(&self) -> Option<Vec<&String>> {
            fn go<'a>(e: &'a LabelExpr, out: &mut Vec<&'a String>) -> bool {
                match e {
                    LabelExpr::Atom(a) => {
                        out.push(a);
                        true
                    }
                    LabelExpr::Or(l, r) => go(l, out) && go(r, out),
                    LabelExpr::And(..) | LabelExpr::Not(..) => false,
                }
            }
            let mut out = Vec::new();
            go(self, &mut out).then_some(out)
        }

        /// The positively-required atoms — labels that must be present for the whole
        /// expression to hold (its conjunctive positive atoms). The planner uses these
        /// to pick a label/index scan; `node_ok` always re-checks the full expression,
        /// so a returned subset only ever *widens* the candidate set (never unsoundly
        /// narrows it). `A&B` → {A,B}; `A|B`, `!A`, `A|(B&C)` → {} for the unguaranteed
        /// parts. For the pre-GQL `:A` / `:A:B` forms this reproduces the old
        /// `labels: Vec<String>` exactly, so existing plans are unchanged.
        pub fn required_atoms(&self, out: &mut Vec<String>) {
            match self {
                LabelExpr::Atom(a) => out.push(a.clone()),
                LabelExpr::And(l, r) => {
                    l.required_atoms(out);
                    r.required_atoms(out);
                }
                LabelExpr::Or(..) | LabelExpr::Not(..) => {}
            }
        }

        /// Evaluate the expression given a predicate that reports whether a named atom
        /// is present (a label on the node, or the relationship's single type). Plain
        /// boolean recursion — no three-valued logic.
        pub fn eval(&self, present: &impl Fn(&str) -> bool) -> bool {
            match self {
                LabelExpr::Atom(a) => present(a),
                LabelExpr::And(l, r) => l.eval(present) && r.eval(present),
                LabelExpr::Or(l, r) => l.eval(present) || r.eval(present),
                LabelExpr::Not(x) => !x.eval(present),
            }
        }
    }

    impl NodePat {
        /// The labels the planner may treat as positively required (see
        /// [`LabelExpr::required_atoms`]). Empty when there is no label constraint or
        /// it is purely disjunctive/negated.
        pub fn required_labels(&self) -> Vec<String> {
            let mut out = Vec::new();
            if let Some(e) = &self.label_expr {
                e.required_atoms(&mut out);
            }
            out
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Direction {
        Outgoing,
        Incoming,
        Undirected,
    }

    /// `*min..max` bounds on a variable-length relationship (each side optional).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct VarLength {
        pub min: Option<u32>,
        pub max: Option<u32>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BinOp {
        Add,
        Sub,
        Mul,
        Div,
        Mod,
        /// Exponentiation (`^`). Always evaluates to a Float (Neo4j semantics).
        Pow,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CmpOp {
        Eq,
        Ne,
        Lt,
        Le,
        Gt,
        Ge,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum StrOp {
        StartsWith,
        EndsWith,
        Contains,
        Regex,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Quantifier {
        Any,
        All,
        None,
        Single,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum FuncArgs {
        /// `count(*)`
        Star,
        Args(Vec<Expr>),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum MapProjItem {
        AllProps,
        Property(String),
        Literal(String, Expr),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Expr {
        Literal(Value),
        Param(String),
        Var(String),
        Property(Box<Expr>, String),
        Index(Box<Expr>, Box<Expr>),
        /// `base[from..to]` slice. Either bound may be absent (open end), in
        /// which case it defaults to the start/end of the sequence.
        Slice {
            base: Box<Expr>,
            from: Option<Box<Expr>>,
            to: Option<Box<Expr>>,
        },
        /// `expr:Label1:Label2` label predicate (boolean).
        HasLabels(Box<Expr>, Vec<String>),
        Neg(Box<Expr>),
        Not(Box<Expr>),
        And(Vec<Expr>),
        Or(Vec<Expr>),
        Xor(Vec<Expr>),
        Arith(BinOp, Box<Expr>, Box<Expr>),
        Compare(CmpOp, Box<Expr>, Box<Expr>),
        StringOp(StrOp, Box<Expr>, Box<Expr>),
        In(Box<Expr>, Box<Expr>),
        /// `expr IS NULL` / `expr IS NOT NULL` (the bool is `negated`).
        IsNull(Box<Expr>, bool),
        Case {
            subject: Option<Box<Expr>>,
            whens: Vec<(Expr, Expr)>,
            els: Option<Box<Expr>>,
        },
        Function {
            name: String,
            distinct: bool,
            args: FuncArgs,
        },
        List(Vec<Expr>),
        Map(Vec<(String, Expr)>),
        MapProjection {
            var: String,
            items: Vec<MapProjItem>,
        },
        ListPredicate {
            quant: Quantifier,
            var: String,
            list: Box<Expr>,
            predicate: Option<Box<Expr>>,
        },
        /// `[var IN list WHERE predicate | projection]`. At least one of
        /// `predicate`/`projection` is present (grammar-enforced); a missing
        /// projection yields the bound element.
        ListComprehension {
            var: String,
            list: Box<Expr>,
            predicate: Option<Box<Expr>>,
            projection: Option<Box<Expr>>,
        },
        /// `[pattern WHERE predicate | projection]` — the pattern is matched
        /// against the surrounding scope's bindings and `projection` is collected
        /// per match (projection is mandatory).
        PatternComprehension {
            pattern: Box<Pattern>,
            predicate: Option<Box<Expr>>,
            projection: Box<Expr>,
        },
        /// `reduce(acc = init, var IN list | body)` — fold `body` over `list`,
        /// threading the accumulator from `init`.
        Reduce {
            acc_var: String,
            acc_init: Box<Expr>,
            var: String,
            list: Box<Expr>,
            body: Box<Expr>,
        },
        /// A bare relationship pattern used as a boolean predicate — true iff the
        /// pattern (seeded by the surrounding bindings) has ≥1 match. The pattern
        /// has at least one relationship and no path variable.
        PatternPredicate(Box<Pattern>),
        /// `EXISTS { [MATCH] patterns [WHERE predicate] }` — true iff the inner
        /// pattern(s), matched against the outer bindings, yield ≥1 row.
        Exists {
            patterns: Vec<Pattern>,
            predicate: Option<Box<Expr>>,
        },
        /// `shortestPath((a)-[*]->(b))` — the shortest path between two already-bound
        /// endpoint nodes over a single variable-length relationship, or NULL when no
        /// path exists. The inner pattern carries exactly one relationship.
        ShortestPath(Box<Pattern>),
    }
}

use ast::*;

/// Parse a read-only Cypher query into its AST. Errors on a syntax error or on a
/// write/procedure clause (Slater is read-only).
pub fn parse(input: &str) -> Result<Query> {
    let mut pairs = CypherParser::parse(Rule::query, input)
        .map_err(|e| anyhow::anyhow!("syntax error: {e}"))?;
    let query = pairs.next().expect("query rule yields one pair");
    lower_query(query)
}

/// Parse a statement that may be a read query **or** a writable-layer write
/// ([`ast::Statement`]). The narrow write grammar (`MATCH … SET …`) is tried
/// first; anything that is not that exact shape falls through to the read parser
/// (so an unsupported write is still rejected there as read-only). The server
/// calls this only when the writable layer is enabled; otherwise it calls
/// [`parse`], which never yields a write.
pub fn parse_statement(input: &str) -> Result<ast::Statement> {
    // `CALL slater.consolidate()` — the Phase 5 consolidation trigger. Its own SOI/EOI
    // anchored rule, so a successful parse means the whole input is exactly that call.
    if CypherParser::parse(Rule::consolidate_call, input).is_ok() {
        return Ok(ast::Statement::Consolidate);
    }
    if let Ok(mut pairs) = CypherParser::parse(Rule::write_statement, input) {
        let stmt = pairs.next().expect("write_statement rule yields one pair");
        // A relationship write (Phase 3c) parses to a single `edge_write` child; a CREATE
        // to a single `create_stmt`; the node write's tokens are inline. Dispatch on which.
        if let Some(edge) = kids(stmt.clone()).find(|c| c.as_rule() == Rule::edge_write) {
            return Ok(ast::Statement::WriteEdge(lower_edge_write(edge)?));
        }
        if let Some(create) = kids(stmt.clone()).find(|c| c.as_rule() == Rule::create_stmt) {
            return Ok(ast::Statement::Create(lower_create_stmt(create)?));
        }
        return Ok(ast::Statement::Write(lower_write_statement(stmt)?));
    }
    parse(input).map(ast::Statement::Read)
}

/// Lower an `edge_write` (`edge_merge` | `edge_delete`) into an [`EdgeWriteStmt`],
/// enforcing the Phase 3c shape: two single-label, single-business-key endpoint node
/// patterns joined by a single directed `-[:R]->` of exactly one relationship type,
/// no variable-length and no edge properties. A `DELETE` names the relationship
/// variable (which the pattern must bind); `MERGE` needs none.
fn lower_edge_write(pair: Pair<Rule>) -> Result<EdgeWriteStmt> {
    let inner = only_child(pair)?; // edge_merge | edge_delete
    let is_merge = inner.as_rule() == Rule::edge_merge;
    let mut src: Option<NodePat> = None;
    let mut dst: Option<NodePat> = None;
    let mut rel: Option<RelPat> = None;
    let mut delete: Option<(bool, String)> = None; // (detach, rel var)
    let mut sets: Vec<(String, String, Expr)> = Vec::new(); // (var, prop, value)
    for c in kids(inner) {
        match c.as_rule() {
            Rule::kw_merge | Rule::kw_match => {}
            Rule::node_pattern => {
                let np = lower_node_pattern(c)?;
                if src.is_none() {
                    src = Some(np);
                } else {
                    dst = Some(np);
                }
            }
            Rule::rel_pattern => rel = Some(lower_rel_pattern(c)?),
            Rule::set_clause => {
                for item in kids(c) {
                    debug_assert_eq!(item.as_rule(), Rule::set_item);
                    let (svar, si) = lower_set_item(item)?;
                    match si {
                        SetItem::Prop { prop, value } => sets.push((svar, prop, value)),
                        SetItem::ReplaceMap(_) | SetItem::MergeMap(_) => bail!(
                            "a relationship write supports only `SET r.prop = value`, not whole-map assignment"
                        ),
                        SetItem::AddLabels(_) => {
                            bail!("relationships have a type, not labels; use `SET r.prop = value`")
                        }
                    }
                }
            }
            Rule::delete_clause => {
                let mut detach = false;
                let mut targets: Vec<String> = Vec::new();
                for d in kids(c) {
                    match d.as_rule() {
                        Rule::kw_detach => detach = true,
                        Rule::kw_delete => {}
                        Rule::var => targets.push(ident_text(only_child(d)?)?),
                        other => bail!("internal: unexpected delete_clause child {other:?}"),
                    }
                }
                if targets.len() != 1 {
                    bail!("a relationship DELETE removes exactly one edge: MATCH (a)-[r:R]->(b) DELETE r");
                }
                delete = Some((detach, targets.into_iter().next().unwrap()));
            }
            other => bail!("internal: unexpected edge_write child {other:?}"),
        }
    }
    let rel = rel.expect("edge_write always has a relationship");
    let src = endpoint(src.expect("edge_write has a source node"), "the source")?;
    let dst = endpoint(
        dst.expect("edge_write has a destination node"),
        "the destination",
    )?;

    if rel.dir != Direction::Outgoing {
        bail!("a write relationship must point left-to-right, e.g. (a)-[:R]->(b)");
    }
    if rel.var_length.is_some() {
        bail!("a write relationship cannot be variable-length");
    }
    if !rel.props.is_empty() {
        bail!("relationship properties are not yet supported in a write");
    }
    let reltype = rel
        .type_expr
        .as_ref()
        .and_then(LabelExpr::as_single_atom)
        .ok_or_else(|| {
            anyhow::anyhow!("a write relationship must carry exactly one type, e.g. (a)-[:R]->(b)")
        })?
        .clone();

    // A `SET` (only the grammar's `edge_merge` alt carries one) must target the bound
    // relationship variable with literal/parameter right-hand sides — the reduced shape
    // of a node write's SET, one edge only.
    let mut out_sets = Vec::with_capacity(sets.len());
    if !sets.is_empty() {
        let rvar = rel.var.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "to SET a relationship property, name the relationship: MERGE (a)-[r:R]->(b) SET r.p = …"
            )
        })?;
        for (set_var, prop, value) in sets {
            if set_var != rvar {
                bail!(
                    "SET must target the relationship variable '{rvar}', not '{set_var}' (a relationship write mutates one edge)"
                );
            }
            ensure_constant(&value, &format!("the value assigned to {rvar}.{prop}"))?;
            out_sets.push((prop, value));
        }
    }

    let op = if is_merge {
        EdgeWriteOp::Create
    } else {
        let (detach, target) = delete.expect("edge_delete has a delete_clause");
        if detach {
            bail!("DETACH deletes a node and its edges; to delete a relationship use MATCH (a)-[r:R]->(b) DELETE r");
        }
        let rvar = rel.var.as_deref().ok_or_else(|| {
            anyhow::anyhow!("to delete a relationship, name it: MATCH (a)-[r:R]->(b) DELETE r")
        })?;
        if target != rvar {
            bail!("DELETE must target the relationship variable '{rvar}', not '{target}'");
        }
        EdgeWriteOp::Delete
    };

    Ok(EdgeWriteStmt {
        src,
        reltype,
        dst,
        op,
        sets: out_sets,
    })
}

/// Reduce a labelled node pattern to an [`EndpointPat`] (single label, exactly one
/// inline business-key property whose value is a literal/parameter) — the endpoint of
/// a relationship write. `what` names the endpoint in error messages.
fn endpoint(node: NodePat, what: &str) -> Result<EndpointPat> {
    let label = node
        .label_expr
        .as_ref()
        .and_then(LabelExpr::as_single_atom)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{what} node of a write must carry exactly one label, e.g. (a:Label {{…}})"
            )
        })?
        .clone();
    if node.props.len() != 1 {
        bail!("{what} node of a write must carry exactly one inline business-key property, e.g. (a:Label {{key: value}})");
    }
    let (key, key_value) = node.props.into_iter().next().unwrap();
    ensure_constant(&key_value, &format!("{what} business-key value"))?;
    Ok(EndpointPat {
        label,
        key,
        key_value,
    })
}

/// Lower a `node_labels` (`:A:B`) to the plain list of label names it carries.
fn lower_node_labels(pair: Pair<Rule>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for c in kids(pair) {
        debug_assert_eq!(c.as_rule(), Rule::label_name);
        out.push(ident_text(c)?);
    }
    debug_assert!(!out.is_empty(), "node_labels matches at least one `:label`");
    Ok(out)
}

/// Lower one `set_item` to its `(target_var, SetItem)`. The target variable is
/// returned separately so the caller can check it against the anchor.
fn lower_set_item(pair: Pair<Rule>) -> Result<(String, SetItem)> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::set_prop => {
            let mut it = kids(inner);
            let var = ident_text(it.next().expect("set_prop has a target variable"))?;
            let prop = ident_text(only_child(
                it.next().expect("set_prop has a property access"),
            )?)?;
            let value = lower_expr(it.next().expect("set_prop has a value expression"))?;
            Ok((var, SetItem::Prop { prop, value }))
        }
        Rule::set_merge_map => {
            let mut it = kids(inner);
            let var = ident_text(it.next().expect("set_merge_map has a target variable"))?;
            let map = lower_prop_map(it.next().expect("set_merge_map has a map literal"))?;
            Ok((var, SetItem::MergeMap(map)))
        }
        Rule::set_replace_map => {
            let mut it = kids(inner);
            let var = ident_text(it.next().expect("set_replace_map has a target variable"))?;
            let map = lower_prop_map(it.next().expect("set_replace_map has a map literal"))?;
            Ok((var, SetItem::ReplaceMap(map)))
        }
        Rule::set_labels => {
            let mut it = kids(inner);
            let var = ident_text(it.next().expect("set_labels has a target variable"))?;
            let labels = lower_node_labels(it.next().expect("set_labels has a label list"))?;
            Ok((var, SetItem::AddLabels(labels)))
        }
        other => bail!("internal: unexpected set_item child {other:?}"),
    }
}

/// Lower one `remove_item` to its `(target_var, RemoveItem)`.
fn lower_remove_item(pair: Pair<Rule>) -> Result<(String, RemoveItem)> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::remove_prop => {
            let mut it = kids(inner);
            let var = ident_text(it.next().expect("remove_prop has a target variable"))?;
            let prop = ident_text(only_child(
                it.next().expect("remove_prop has a property access"),
            )?)?;
            Ok((var, RemoveItem::Prop(prop)))
        }
        Rule::remove_labels => {
            let mut it = kids(inner);
            let var = ident_text(it.next().expect("remove_labels has a target variable"))?;
            let labels = lower_node_labels(it.next().expect("remove_labels has a label list"))?;
            Ok((var, RemoveItem::Labels(labels)))
        }
        other => bail!("internal: unexpected remove_item child {other:?}"),
    }
}

/// Lower one `update_clause` (`set_clause` | `remove_clause` | `delete_clause`) into the
/// caller's accumulators.
fn lower_update_clause(
    clause: Pair<Rule>,
    set_items: &mut Vec<(String, SetItem)>,
    remove_items: &mut Vec<(String, RemoveItem)>,
    delete: &mut Option<(bool, Vec<String>)>,
) -> Result<()> {
    let uc = only_child(clause)?;
    match uc.as_rule() {
        Rule::set_clause => {
            for item in kids(uc) {
                debug_assert_eq!(item.as_rule(), Rule::set_item);
                set_items.push(lower_set_item(item)?);
            }
        }
        Rule::remove_clause => {
            for item in kids(uc) {
                debug_assert_eq!(item.as_rule(), Rule::remove_item);
                remove_items.push(lower_remove_item(item)?);
            }
        }
        Rule::delete_clause => {
            let mut detach = false;
            let mut targets = Vec::new();
            for c in kids(uc) {
                match c.as_rule() {
                    Rule::kw_detach => detach = true,
                    Rule::kw_delete => {}
                    Rule::var => targets.push(ident_text(only_child(c)?)?),
                    other => bail!("internal: unexpected delete_clause child {other:?}"),
                }
            }
            debug_assert!(!targets.is_empty(), "delete_clause names a variable");
            *delete = Some((detach, targets));
        }
        other => bail!("internal: unexpected update_clause child {other:?}"),
    }
    Ok(())
}

/// Lower a `merge_action` (`ON CREATE SET …` / `ON MATCH SET …`) into `(is_create, items)`.
/// The `ON` and the branch keyword are read from the unfiltered inner (the branch keyword
/// distinguishes create from match).
fn lower_merge_action(pair: Pair<Rule>) -> Result<(bool, Vec<(String, SetItem)>)> {
    let mut is_create = false;
    let mut items = Vec::new();
    for c in pair.into_inner() {
        match c.as_rule() {
            Rule::kw_on | Rule::kw_match => {}
            Rule::kw_create => is_create = true,
            Rule::set_clause => {
                for item in kids(c) {
                    items.push(lower_set_item(item)?);
                }
            }
            _ => {}
        }
    }
    Ok((is_create, items))
}

/// Lower a `create_stmt` (`CREATE (n:Label {props})`) into a [`CreateStmt`]. The node must
/// be named and single-labelled with at least one inline property (the business key is
/// designated by the server, which knows the range index); property values must be
/// constants (literal or parameter).
fn lower_create_stmt(pair: Pair<Rule>) -> Result<CreateStmt> {
    let mut node: Option<NodePat> = None;
    let mut ret: Option<ReturnClause> = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::kw_create => {}
            Rule::node_pattern => node = Some(lower_node_pattern(child)?),
            Rule::return_clause => ret = Some(lower_return_clause(child)?),
            Rule::EOI => {}
            other => bail!("internal: unexpected create_stmt child {other:?}"),
        }
    }
    let node = node.expect("create_stmt always has a node pattern");
    let var = node.var.ok_or_else(|| {
        anyhow::anyhow!("a CREATE node must be named, e.g. CREATE (n:Label {{…}})")
    })?;
    let label = node
        .label_expr
        .as_ref()
        .and_then(LabelExpr::as_single_atom)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "a CREATE node must carry exactly one label, e.g. CREATE (n:Label {{…}})"
            )
        })?
        .clone();
    if node.props.is_empty() {
        bail!("a CREATE node must carry at least one inline property (its business key), e.g. CREATE (n:Label {{key: value}})");
    }
    for (name, value) in &node.props {
        ensure_constant(value, &format!("the value of {var}.{name}"))?;
    }
    Ok(CreateStmt {
        var,
        label,
        props: node.props,
        ret,
    })
}

/// Lower a `write_statement` into a [`WriteStmt`], enforcing the Phase 1c shape:
/// a single labelled anchor node with exactly one inline business-key property
/// (a literal or parameter), and SET assignments that all target that anchor
/// variable with literal/parameter right-hand sides. Anything outside the shape
/// is a clear error rather than a silent mis-parse.
fn lower_write_statement(pair: Pair<Rule>) -> Result<WriteStmt> {
    let mut node: Option<NodePat> = None;
    let mut set_items: Vec<(String, SetItem)> = Vec::new();
    let mut remove_items: Vec<(String, RemoveItem)> = Vec::new();
    let mut delete: Option<(bool, Vec<String>)> = None; // (detach, target vars)
    let mut on_create_items: Vec<(String, SetItem)> = Vec::new();
    let mut on_match_items: Vec<(String, SetItem)> = Vec::new();
    let mut ret: Option<ReturnClause> = None;
    let mut upsert = false; // MERGE anchor (create-if-absent) vs MATCH (update only)
    let mut unwind: Option<(Expr, String)> = None; // write-UNWIND source + alias
    for child in kids(pair) {
        match child.as_rule() {
            Rule::unwind_clause => {
                let uw = lower_unwind_clause(child)?;
                unwind = Some((uw.expr, uw.var));
            }
            Rule::kw_match => {}
            Rule::kw_merge => upsert = true,
            Rule::node_pattern => node = Some(lower_node_pattern(child)?),
            Rule::write_actions => {
                for act in kids(child) {
                    match act.as_rule() {
                        Rule::merge_action => {
                            let (is_create, items) = lower_merge_action(act)?;
                            if is_create {
                                on_create_items.extend(items);
                            } else {
                                on_match_items.extend(items);
                            }
                        }
                        Rule::update_clause => lower_update_clause(
                            act,
                            &mut set_items,
                            &mut remove_items,
                            &mut delete,
                        )?,
                        other => bail!("internal: unexpected write_actions child {other:?}"),
                    }
                }
            }
            Rule::return_clause => ret = Some(lower_return_clause(child)?),
            Rule::EOI => {}
            other => bail!("internal: unexpected write_statement child {other:?}"),
        }
    }

    let node = node.expect("write_statement always has a node pattern");
    let var = node.var.ok_or_else(|| {
        anyhow::anyhow!("a write's anchor node must be named, e.g. (n:Label {{…}})")
    })?;
    let label = node
        .label_expr
        .as_ref()
        .and_then(LabelExpr::as_single_atom)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "a write's anchor node must carry exactly one label, e.g. (n:Label {{…}})"
            )
        })?
        .clone();
    if node.props.len() != 1 {
        bail!("a write's anchor node must carry exactly one inline business-key property, e.g. (n:Label {{key: value}})");
    }
    let (key, key_value) = node.props.into_iter().next().unwrap();
    // A plain write's values must be constants; a write-UNWIND's may reference the
    // alias's fields (validated per-row at execution).
    if unwind.is_none() {
        ensure_constant(&key_value, "the anchor business-key value")?;
    }

    // Exactly one update clause fired (the grammar alternates SET / REMOVE / DELETE).
    let op = if let Some((detach, targets)) = delete {
        if upsert {
            bail!("MERGE cannot be combined with DELETE — use MATCH … DELETE to remove a node");
        }
        for target in &targets {
            if target != &var {
                bail!(
                    "DELETE must target the anchor variable '{var}', not '{target}' (a write anchors one node)"
                );
            }
        }
        WriteOp::Delete { detach, targets }
    } else if !remove_items.is_empty() {
        let mut out = Vec::with_capacity(remove_items.len());
        for (rvar, item) in remove_items {
            if rvar != var {
                bail!(
                    "REMOVE must target the anchor variable '{var}', not '{rvar}' (a write mutates one node)"
                );
            }
            out.push(item);
        }
        WriteOp::Remove(out)
    } else {
        // A bare MERGE with only ON CREATE/ON MATCH blocks has no unconditional SET.
        if set_items.is_empty() && on_create_items.is_empty() && on_match_items.is_empty() {
            bail!("a write must SET or REMOVE at least one property or label");
        }
        let mut out = Vec::with_capacity(set_items.len());
        for (svar, item) in set_items {
            if svar != var {
                bail!(
                    "SET must target the anchor variable '{var}', not '{svar}' (a write mutates one node)"
                );
            }
            // A plain write's values must be constants; a write-UNWIND's may reference
            // the alias's fields (validated per-row at execution).
            if unwind.is_none() {
                ensure_set_item_constant(&item, &var)?;
            }
            out.push(item);
        }
        WriteOp::Set(out)
    };

    let is_unwind = unwind.is_some();
    let on_create = validate_conditional_sets(on_create_items, &var, upsert, is_unwind, "CREATE")?;
    let on_match = validate_conditional_sets(on_match_items, &var, upsert, is_unwind, "MATCH")?;

    Ok(WriteStmt {
        var,
        label,
        key,
        key_value,
        upsert,
        op,
        on_create,
        on_match,
        ret,
        unwind,
    })
}

/// Validate a MERGE's `ON CREATE`/`ON MATCH` SET items: only valid on a MERGE, each must
/// target the anchor variable, and (for a non-UNWIND write) carry constant values. Returns
/// the bare [`SetItem`]s in source order.
fn validate_conditional_sets(
    items: Vec<(String, SetItem)>,
    var: &str,
    upsert: bool,
    is_unwind: bool,
    which: &str,
) -> Result<Vec<SetItem>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    if !upsert {
        bail!("ON {which} SET is only valid on MERGE, not MATCH");
    }
    let mut out = Vec::with_capacity(items.len());
    for (svar, item) in items {
        if svar != var {
            bail!("ON {which} SET must target the anchor variable '{var}', not '{svar}'");
        }
        if !is_unwind {
            ensure_set_item_constant(&item, var)?;
        }
        out.push(item);
    }
    Ok(out)
}

/// A plain (non-UNWIND) write's SET right-hand sides must be values known without
/// reading the graph — a literal or a parameter (or such inside a replace/merge map).
/// Label-set items carry no expression, so they always pass.
fn ensure_set_item_constant(item: &SetItem, var: &str) -> Result<()> {
    match item {
        SetItem::Prop { prop, value } => {
            ensure_constant(value, &format!("the value assigned to {var}.{prop}"))
        }
        SetItem::ReplaceMap(map) | SetItem::MergeMap(map) => {
            for (k, v) in map {
                ensure_constant(v, &format!("the value assigned to {var}.{k}"))?;
            }
            Ok(())
        }
        SetItem::AddLabels(_) => Ok(()),
    }
}

/// A Phase 1c write's business-key value and every SET right-hand side must be a
/// value known without reading the graph: a literal or a parameter. (Computed
/// right-hand sides such as `n.x + 1` land in a later phase.)
fn ensure_constant(e: &Expr, what: &str) -> Result<()> {
    match e {
        Expr::Literal(_) | Expr::Param(_) => Ok(()),
        _ => bail!("{what} must be a literal or a parameter in a Phase 1c write"),
    }
}

/// Builtins whose result depends on the wall clock or an entropy source. A query
/// calling any of these must be excluded from the result cache. Lowercased.
const NONDETERMINISTIC_FUNCTIONS: &[&str] = &["rand", "randomuuid", "timestamp"];

/// Whether `query` calls a non-deterministic builtin (`rand`/`randomUUID`/
/// `timestamp`) anywhere — inside `WHERE`/`WITH`/`RETURN`/`ORDER BY` expressions,
/// pattern property maps, comprehensions, or nested `CALL { … }` subqueries. The
/// server uses this to skip the result-cache get *and* insert, so each run
/// re-evaluates the clock/RNG (otherwise a cache hit would replay a stale value).
///
/// The `Expr` walk below is deliberately exhaustive (no `_` arm): a new `Expr`
/// variant will fail to compile here until it is classified, so the detector
/// can never silently miss a place a function call can hide.
pub fn is_nondeterministic(query: &Query) -> bool {
    single_query_nd(&query.head) || query.tail.iter().any(|(_, sq)| single_query_nd(sq))
}

fn single_query_nd(sq: &SingleQuery) -> bool {
    sq.reading.iter().any(clause_nd) || projection_body_nd(&sq.ret.body)
}

fn clause_nd(c: &Clause) -> bool {
    match c {
        Clause::Match(m) => {
            m.patterns.iter().any(pattern_nd) || m.where_.as_ref().is_some_and(expr_nd)
        }
        Clause::With(w) => projection_body_nd(&w.body) || w.where_.as_ref().is_some_and(expr_nd),
        Clause::VectorCall(v) => {
            expr_nd(&v.k) || expr_nd(&v.query_vec) || v.where_.as_ref().is_some_and(expr_nd)
        }
        Clause::Call(c) => c.args.iter().any(expr_nd) || c.where_.as_ref().is_some_and(expr_nd),
        Clause::CallSubquery(c) => is_nondeterministic(&c.inner),
        Clause::Unwind(u) => expr_nd(&u.expr),
    }
}

fn projection_body_nd(b: &ProjectionBody) -> bool {
    b.items.iter().any(|it| expr_nd(&it.expr))
        || b.order_by.iter().any(|(e, _)| expr_nd(e))
        || b.skip.as_ref().is_some_and(expr_nd)
        || b.limit.as_ref().is_some_and(expr_nd)
}

fn pattern_nd(p: &Pattern) -> bool {
    node_pat_nd(&p.start) || p.rels.iter().any(|(r, n)| rel_pat_nd(r) || node_pat_nd(n))
}

fn node_pat_nd(n: &NodePat) -> bool {
    n.props.iter().any(|(_, e)| expr_nd(e))
}

fn rel_pat_nd(r: &RelPat) -> bool {
    r.props.iter().any(|(_, e)| expr_nd(e))
}

fn expr_nd(e: &Expr) -> bool {
    match e {
        Expr::Function { name, args, .. } => {
            NONDETERMINISTIC_FUNCTIONS.contains(&name.to_lowercase().as_str())
                || match args {
                    FuncArgs::Star => false,
                    FuncArgs::Args(a) => a.iter().any(expr_nd),
                }
        }
        Expr::Literal(_) | Expr::Param(_) | Expr::Var(_) => false,
        Expr::Property(b, _) | Expr::HasLabels(b, _) | Expr::IsNull(b, _) => expr_nd(b),
        Expr::Neg(b) | Expr::Not(b) => expr_nd(b),
        Expr::Index(a, b)
        | Expr::Arith(_, a, b)
        | Expr::Compare(_, a, b)
        | Expr::StringOp(_, a, b)
        | Expr::In(a, b) => expr_nd(a) || expr_nd(b),
        Expr::Slice { base, from, to } => {
            expr_nd(base)
                || from.as_deref().is_some_and(expr_nd)
                || to.as_deref().is_some_and(expr_nd)
        }
        Expr::And(xs) | Expr::Or(xs) | Expr::Xor(xs) | Expr::List(xs) => xs.iter().any(expr_nd),
        Expr::Case {
            subject,
            whens,
            els,
        } => {
            subject.as_deref().is_some_and(expr_nd)
                || whens.iter().any(|(w, t)| expr_nd(w) || expr_nd(t))
                || els.as_deref().is_some_and(expr_nd)
        }
        Expr::Map(kvs) => kvs.iter().any(|(_, v)| expr_nd(v)),
        Expr::MapProjection { items, .. } => items.iter().any(|it| match it {
            MapProjItem::Literal(_, e) => expr_nd(e),
            MapProjItem::AllProps | MapProjItem::Property(_) => false,
        }),
        Expr::ListPredicate {
            list, predicate, ..
        } => expr_nd(list) || predicate.as_deref().is_some_and(expr_nd),
        Expr::ListComprehension {
            list,
            predicate,
            projection,
            ..
        } => {
            expr_nd(list)
                || predicate.as_deref().is_some_and(expr_nd)
                || projection.as_deref().is_some_and(expr_nd)
        }
        Expr::PatternComprehension {
            pattern,
            predicate,
            projection,
        } => {
            pattern_nd(pattern) || predicate.as_deref().is_some_and(expr_nd) || expr_nd(projection)
        }
        Expr::Reduce {
            acc_init,
            list,
            body,
            ..
        } => expr_nd(acc_init) || expr_nd(list) || expr_nd(body),
        Expr::PatternPredicate(p) | Expr::ShortestPath(p) => pattern_nd(p),
        Expr::Exists {
            patterns,
            predicate,
        } => patterns.iter().any(pattern_nd) || predicate.as_deref().is_some_and(expr_nd),
    }
}

// ── Clause lowering ──────────────────────────────────────────────────────────

fn lower_query(pair: Pair<Rule>) -> Result<Query> {
    let mut head: Option<SingleQuery> = None;
    let mut tail: Vec<(bool, SingleQuery)> = Vec::new();
    let mut pending_union_all: Option<bool> = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::single_query => {
                let sq = lower_single_query(child)?;
                match pending_union_all.take() {
                    Some(all) => tail.push((all, sq)),
                    None if head.is_none() => head = Some(sq),
                    None => bail!("internal: two query parts without a UNION"),
                }
            }
            Rule::union => {
                // `union = { "union" ~ "all"? }` — neither keyword is a captured
                // child, so detect `ALL` from the matched text.
                let all = child
                    .as_str()
                    .split_whitespace()
                    .any(|w| w.eq_ignore_ascii_case("all"));
                pending_union_all = Some(all);
            }
            Rule::EOI => {}
            other => bail!("internal: unexpected query child {other:?}"),
        }
    }
    Ok(Query {
        head: head.ok_or_else(|| anyhow::anyhow!("empty query"))?,
        tail,
    })
}

fn lower_single_query(pair: Pair<Rule>) -> Result<SingleQuery> {
    let mut reading = Vec::new();
    let mut ret = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::forbidden_query => {
                let kw = child
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::forbidden_clause)
                    .map(|p| p.as_str().to_uppercase())
                    .unwrap_or_else(|| "WRITE".to_string());
                bail!("Slater is read-only; the '{kw}' clause is not permitted");
            }
            Rule::reading_clause => reading.push(lower_reading_clause(child)?),
            Rule::return_clause => ret = Some(lower_return_clause(child)?),
            // A bare standalone `CALL proc()` (no trailing RETURN): the call is the
            // whole query, so synthesise a `RETURN *` over its yielded outputs.
            Rule::call_clause => {
                reading.push(Clause::Call(lower_call_clause(child)?));
                ret = Some(star_return());
            }
            other => bail!("internal: unexpected single_query child {other:?}"),
        }
    }
    Ok(SingleQuery {
        reading,
        ret: ret.ok_or_else(|| anyhow::anyhow!("query has no RETURN"))?,
    })
}

/// A synthetic `RETURN *` — used for a bare metadata `CALL proc()` whose result
/// columns are exactly the procedure's yielded outputs.
fn star_return() -> ReturnClause {
    ReturnClause {
        distinct: false,
        body: ProjectionBody {
            star: true,
            items: Vec::new(),
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
    }
}

fn lower_reading_clause(pair: Pair<Rule>) -> Result<Clause> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::match_clause => Ok(Clause::Match(lower_match_clause(inner)?)),
        Rule::with_clause => Ok(Clause::With(lower_with_clause(inner)?)),
        Rule::vector_call_clause => Ok(Clause::VectorCall(lower_vector_call(inner)?)),
        Rule::call_clause => Ok(Clause::Call(lower_call_clause(inner)?)),
        Rule::call_subquery => Ok(Clause::CallSubquery(lower_call_subquery(inner)?)),
        Rule::unwind_clause => Ok(Clause::Unwind(lower_unwind_clause(inner)?)),
        // GQL `FOR alias IN expr` lowers onto the very same UnwindClause — see
        // `lower_for_clause`. The executor never learns the surface spelling.
        Rule::for_clause => Ok(Clause::Unwind(lower_for_clause(inner)?)),
        other => bail!("internal: unexpected reading clause {other:?}"),
    }
}

/// Error text matching FalkorDB's `EMSG_CALLSUBQUERY_INVALID_REFERENCES`.
const IMPORT_ERR: &str =
    "WITH imports in CALL {} must consist of only simple references to outside variables";

/// Lower a `CALL { <subquery> }` clause. The inner body is one or more
/// `UNION`-joined parts; each part lowers to a [`SingleQuery`] plus its
/// [`Imports`] (the simple variables its leading `WITH` brings in) and whether it
/// returns. Mirrors FalkorDB `_Validate_call_subquery`: every branch is validated
/// independently, and the branches must agree on returning vs. unit.
fn lower_call_subquery(pair: Pair<Rule>) -> Result<CallSubqueryClause> {
    let subquery = kids(pair)
        .find(|p| p.as_rule() == Rule::subquery)
        .ok_or_else(|| anyhow::anyhow!("internal: CALL {{}} has no subquery body"))?;

    let mut parts: Vec<(SingleQuery, bool, Imports)> = Vec::new();
    let mut union_all: Vec<bool> = Vec::new();
    for child in subquery.into_inner() {
        match child.as_rule() {
            Rule::subquery_part => parts.push(lower_subquery_part(child)?),
            Rule::union => {
                let all = child
                    .as_str()
                    .split_whitespace()
                    .any(|w| w.eq_ignore_ascii_case("all"));
                union_all.push(all);
            }
            other => bail!("internal: unexpected subquery child {other:?}"),
        }
    }
    if parts.is_empty() {
        bail!("internal: CALL {{}} subquery has no parts");
    }

    // All branches must agree: either every branch returns, or it is a single
    // unit (non-returning) branch. FalkorDB rejects a mixed/union unit subquery.
    let returning = parts[0].1;
    if parts.iter().any(|(_, r, _)| *r != returning) {
        bail!("all branches of a CALL {{}} subquery must return, or none may");
    }
    if !returning && parts.len() > 1 {
        bail!("a non-returning CALL {{}} subquery cannot use UNION");
    }

    let mut imports = Vec::with_capacity(parts.len());
    let mut sqs: Vec<SingleQuery> = Vec::with_capacity(parts.len());
    for (sq, _, imp) in parts {
        imports.push(imp);
        sqs.push(sq);
    }
    let head = sqs.remove(0);
    let tail: Vec<(bool, SingleQuery)> = union_all.into_iter().zip(sqs).collect();

    Ok(CallSubqueryClause {
        inner: Box::new(Query { head, tail }),
        imports,
        returning,
    })
}

/// Lower one `subquery_part` into a [`SingleQuery`], whether it returns, and its
/// imported outer variables. A part with no `RETURN` (a unit subquery) is given a
/// synthetic `RETURN *` placeholder that exec never projects.
fn lower_subquery_part(pair: Pair<Rule>) -> Result<(SingleQuery, bool, Imports)> {
    let mut reading: Vec<Clause> = Vec::new();
    let mut ret = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::subquery_forbidden => {
                let kw = child
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::forbidden_clause)
                    .map(|p| p.as_str().to_uppercase())
                    .unwrap_or_else(|| "WRITE".to_string());
                bail!("Slater is read-only; the '{kw}' clause is not permitted in CALL {{}}");
            }
            Rule::reading_clause => reading.push(lower_reading_clause(child)?),
            Rule::return_clause => ret = Some(lower_return_clause(child)?),
            other => bail!("internal: unexpected subquery_part child {other:?}"),
        }
    }
    let returning = ret.is_some();
    let imports = import_spec(reading.first())?;
    let ret = ret.unwrap_or_else(star_return);
    Ok((SingleQuery { reading, ret }, returning, imports))
}

/// Determine what a subquery branch imports from its leading clause, validating
/// the FalkorDB "simple references" rule when that clause is a `WITH`.
fn import_spec(first: Option<&Clause>) -> Result<Imports> {
    let Some(Clause::With(w)) = first else {
        return Ok(Imports::None);
    };
    // A leading import WITH may not carry ORDER BY / SKIP / LIMIT / WHERE.
    if w.where_.is_some()
        || !w.body.order_by.is_empty()
        || w.body.skip.is_some()
        || w.body.limit.is_some()
    {
        bail!("{IMPORT_ERR}");
    }
    // `WITH *` imports every outer variable; FalkorDB skips the per-item check for
    // the star form.
    if w.body.star {
        return Ok(Imports::All);
    }
    let mut names = Vec::with_capacity(w.body.items.len());
    for it in &w.body.items {
        match (&it.expr, &it.alias) {
            (Expr::Var(n), None) => names.push(n.clone()),
            _ => bail!("{IMPORT_ERR}"),
        }
    }
    Ok(Imports::Named(names))
}

/// Lower a read-only metadata `call_clause` into a [`CallClause`]. Mirrors
/// [`lower_vector_call`]'s YIELD/WHERE collection but for argument-less procedures
/// whose outputs are fixed by name.
fn lower_call_clause(pair: Pair<Rule>) -> Result<CallClause> {
    let mut name = String::new();
    let mut args: Vec<Expr> = Vec::new();
    let mut yields: Vec<(String, String)> = Vec::new();
    let mut where_ = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::read_proc => name = child.as_str().to_string(),
            Rule::where_clause => where_ = Some(lower_expr(only_child(child)?)?),
            Rule::func_arg => {
                let inner = only_child(child)?;
                if inner.as_rule() == Rule::star_arg {
                    bail!("procedure {name} does not take '*' as an argument");
                }
                args.push(lower_expr(inner)?);
            }
            Rule::yield_clause => {
                for item in kids(child) {
                    if item.as_rule() != Rule::yield_item {
                        continue;
                    }
                    let mut it = kids(item);
                    let output = ident_text(it.next().unwrap())?;
                    let bound = it
                        .next()
                        .map(ident_text)
                        .transpose()?
                        .unwrap_or_else(|| output.clone());
                    yields.push((output, bound));
                }
            }
            other => bail!("internal: unexpected call_clause child {other:?}"),
        }
    }
    Ok(CallClause {
        name,
        args,
        yields,
        where_,
    })
}

fn lower_vector_call(pair: Pair<Rule>) -> Result<VectorCallClause> {
    let mut args: Vec<Expr> = Vec::new();
    let mut yields: Vec<(String, String)> = Vec::new();
    let mut where_ = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::vector_proc => {}
            Rule::where_clause => where_ = Some(lower_expr(only_child(child)?)?),
            Rule::func_arg => {
                let inner = only_child(child)?;
                if inner.as_rule() == Rule::star_arg {
                    bail!("db.idx.vector.queryNodes does not take '*' as an argument");
                }
                args.push(lower_expr(inner)?);
            }
            Rule::yield_clause => {
                for item in kids(child) {
                    if item.as_rule() != Rule::yield_item {
                        continue;
                    }
                    let mut it = kids(item);
                    let output = ident_text(it.next().unwrap())?;
                    let bound = it
                        .next()
                        .map(ident_text)
                        .transpose()?
                        .unwrap_or_else(|| output.clone());
                    yields.push((output, bound));
                }
            }
            other => bail!("internal: unexpected vector_call child {other:?}"),
        }
    }
    if args.len() != 4 {
        bail!(
            "db.idx.vector.queryNodes expects 4 arguments (label, property, k, queryVector), got {}",
            args.len()
        );
    }
    let mut args = args.into_iter();
    let label = string_arg(args.next().unwrap(), "label")?;
    let property = string_arg(args.next().unwrap(), "property")?;
    let k = args.next().unwrap();
    let query_vec = args.next().unwrap();
    for (output, _) in &yields {
        if output != "node" && output != "score" {
            bail!("db.idx.vector.queryNodes only yields 'node' and 'score', not '{output}'");
        }
    }
    Ok(VectorCallClause {
        label,
        property,
        k,
        query_vec,
        yields,
        where_,
    })
}

/// Require a vector-procedure argument to be a string literal (the label/property
/// names are constants in every observed call), returning its value.
fn string_arg(e: Expr, which: &str) -> Result<String> {
    match e {
        Expr::Literal(Value::Str(s)) => Ok(s),
        other => bail!("db.idx.vector.queryNodes {which} must be a string literal, got {other:?}"),
    }
}

fn lower_match_clause(pair: Pair<Rule>) -> Result<MatchClause> {
    let optional = pair
        .as_str()
        .trim_start()
        .to_lowercase()
        .starts_with("optional");
    let mut patterns = Vec::new();
    let mut where_ = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::match_pattern => patterns.push(lower_match_pattern(child)?),
            Rule::where_clause => where_ = Some(lower_expr(only_child(child)?)?),
            other => bail!("internal: unexpected match child {other:?}"),
        }
    }
    Ok(MatchClause {
        optional,
        patterns,
        where_,
    })
}

fn lower_unwind_clause(pair: Pair<Rule>) -> Result<UnwindClause> {
    // unwind_clause = { kw_unwind ~ expr ~ kw_as ~ alias } — kids() drops the
    // keyword tokens, leaving the list expression then the alias identifier.
    let mut it = kids(pair);
    let expr = lower_expr(
        it.next()
            .ok_or_else(|| anyhow::anyhow!("UNWIND without expression"))?,
    )?;
    let var = ident_text(
        it.next()
            .ok_or_else(|| anyhow::anyhow!("UNWIND without alias"))?,
    )?;
    Ok(UnwindClause { expr, var })
}

fn lower_for_clause(pair: Pair<Rule>) -> Result<UnwindClause> {
    // for_clause = { kw_for ~ alias ~ kw_in ~ expr } — GQL's spelling of UNWIND with
    // the operands reversed. kids() drops the keyword tokens, leaving the alias then
    // the list expression (the opposite order to `lower_unwind_clause`). It returns
    // the identical UnwindClause, so `FOR x IN list` and `UNWIND list AS x` are the
    // same query past the parser.
    let mut it = kids(pair);
    let var = ident_text(
        it.next()
            .ok_or_else(|| anyhow::anyhow!("FOR without alias"))?,
    )?;
    let expr = lower_expr(
        it.next()
            .ok_or_else(|| anyhow::anyhow!("FOR without expression"))?,
    )?;
    Ok(UnwindClause { expr, var })
}

fn lower_with_clause(pair: Pair<Rule>) -> Result<WithClause> {
    let distinct = has_keyword(&pair, "distinct");
    let mut body = None;
    let mut where_ = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::projection_body => body = Some(lower_projection_body(child)?),
            Rule::where_clause => where_ = Some(lower_expr(only_child(child)?)?),
            _ => {}
        }
    }
    Ok(WithClause {
        distinct,
        body: body.ok_or_else(|| anyhow::anyhow!("WITH without projection"))?,
        where_,
    })
}

fn lower_return_clause(pair: Pair<Rule>) -> Result<ReturnClause> {
    let distinct = has_keyword(&pair, "distinct");
    let body = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::projection_body)
        .ok_or_else(|| anyhow::anyhow!("RETURN without projection"))?;
    Ok(ReturnClause {
        distinct,
        body: lower_projection_body(body)?,
    })
}

fn lower_projection_body(pair: Pair<Rule>) -> Result<ProjectionBody> {
    let mut star = false;
    let mut items = Vec::new();
    let mut order_by = Vec::new();
    let mut skip = None;
    let mut limit = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::projection_items => {
                for it in child.into_inner() {
                    match it.as_rule() {
                        Rule::star_item => {
                            star = true;
                            for pi in it.into_inner() {
                                items.push(lower_proj_item(pi)?);
                            }
                        }
                        Rule::projection_item => items.push(lower_proj_item(it)?),
                        other => bail!("internal: unexpected projection item {other:?}"),
                    }
                }
            }
            Rule::order_by => {
                for si in kids(child) {
                    order_by.push(lower_sort_item(si)?);
                }
            }
            Rule::skip => skip = Some(lower_expr(only_child(child)?)?),
            Rule::limit => limit = Some(lower_expr(only_child(child)?)?),
            other => bail!("internal: unexpected projection_body child {other:?}"),
        }
    }
    Ok(ProjectionBody {
        star,
        items,
        order_by,
        skip,
        limit,
    })
}

fn lower_proj_item(pair: Pair<Rule>) -> Result<ProjItem> {
    let mut inner = kids(pair);
    let expr = lower_expr(inner.next().unwrap())?;
    let alias = inner.next().map(ident_text);
    Ok(ProjItem {
        expr,
        alias: alias.transpose()?,
    })
}

fn lower_sort_item(pair: Pair<Rule>) -> Result<(Expr, SortDir)> {
    let text = pair.as_str().to_lowercase();
    let dir = if text.contains("desc") {
        SortDir::Desc
    } else {
        SortDir::Asc
    };
    let expr = lower_expr(kids(pair).next().unwrap())?;
    Ok((expr, dir))
}

// ── Pattern lowering ─────────────────────────────────────────────────────────

/// Flatten a (possibly parenthesised) `pattern_element` / `match_element` into its
/// ordered leaves. openCypher lets a pattern element be wrapped in redundant
/// parentheses to any depth (`MATCH (((a)-[:R]->(b)))`); the nesting groups but
/// carries no meaning, so the leaves are all the lowering needs. `wrapper` names
/// the self-recursive rule, which is the only structural difference between the
/// plain and the quantifier-bearing spellings.
fn flatten_pattern_element(pair: Pair<Rule>, wrapper: Rule) -> Result<Vec<Pair<Rule>>> {
    let mut out = Vec::new();
    push_pattern_leaves(pair, wrapper, &mut out)?;
    Ok(out)
}

fn push_pattern_leaves<'i>(
    pair: Pair<'i, Rule>,
    wrapper: Rule,
    out: &mut Vec<Pair<'i, Rule>>,
) -> Result<()> {
    for child in pair.into_inner() {
        let rule = child.as_rule();
        if rule == wrapper {
            push_pattern_leaves(child, wrapper, out)?;
        } else if matches!(
            rule,
            Rule::node_pattern | Rule::rel_pattern | Rule::quantified_path
        ) {
            out.push(child);
        } else {
            bail!("internal: unexpected pattern element child {rule:?}");
        }
    }
    Ok(())
}

fn lower_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut path_var = None;
    let mut nodes = Vec::new();
    let mut rels = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::path_var => path_var = Some(ident_text(child)?),
            Rule::pattern_element => {
                for elem in flatten_pattern_element(child, Rule::pattern_element)? {
                    match elem.as_rule() {
                        Rule::node_pattern => nodes.push(lower_node_pattern(elem)?),
                        Rule::rel_pattern => rels.push(lower_rel_pattern(elem)?),
                        other => bail!("internal: unexpected pattern element {other:?}"),
                    }
                }
            }
            other => bail!("internal: unexpected pattern child {other:?}"),
        }
    }
    let mut nodes = nodes.into_iter();
    let start = nodes
        .next()
        .ok_or_else(|| anyhow::anyhow!("pattern has no node"))?;
    let mut chain = Vec::new();
    for (rel, node) in rels.into_iter().zip(nodes) {
        chain.push((rel, node));
    }
    Ok(Pattern {
        path_var,
        start,
        rels: chain,
        segments: None,
        restrictor: None,
        selector: None,
    })
}

/// Lower a `match_pattern` (a `pattern` that may contain GQL quantified groups).
/// When no quantified group is present this is exactly [`lower_pattern`]'s result
/// (`segments: None`); when one or more groups appear, the ordered element
/// sequence is captured in `segments` and `rels` is left empty for the executor to
/// desugar.
fn lower_match_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut path_var = None;
    let mut start: Option<NodePat> = None;
    // Pending relationship awaiting its end node, so we can pair `(connector, node)`.
    let mut pending_rel: Option<RelPat> = None;
    let mut pending_quant: Option<(Vec<(RelPat, NodePat)>, VarLength)> = None;
    let mut segments: Vec<Segment> = Vec::new();
    let mut has_quant = false;
    let mut restrictor: Option<PathRestrictor> = None;
    let mut selector: Option<PathSelector> = None;

    let mut elems: Vec<Pair<Rule>> = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            // `MATCH p = shortestPath((a)-[:R*]->(b))` — desugar the openCypher
            // function form to the GQL selector machinery (see `lower_shortest_call`).
            Rule::shortest_call => return lower_shortest_call(child),
            Rule::path_selector => selector = Some(lower_path_selector(child)?),
            Rule::path_restrictor => restrictor = Some(lower_path_restrictor(child)?),
            Rule::path_var => path_var = Some(ident_text(child)?),
            Rule::match_element => elems = flatten_pattern_element(child, Rule::match_element)?,
            other => bail!("internal: unexpected match_pattern child {other:?}"),
        }
    }

    for child in elems {
        match child.as_rule() {
            Rule::node_pattern => {
                let node = lower_node_pattern(child)?;
                if start.is_none() {
                    start = Some(node);
                } else if let Some((inner, bounds)) = pending_quant.take() {
                    segments.push(Segment::Quantified {
                        inner,
                        bounds,
                        exit: node,
                    });
                } else if let Some(rel) = pending_rel.take() {
                    segments.push(Segment::Hop(rel, node));
                } else {
                    bail!("internal: pattern node without a preceding connector");
                }
            }
            Rule::rel_pattern => pending_rel = Some(lower_rel_pattern(child)?),
            Rule::quantified_path => {
                has_quant = true;
                pending_quant = Some(lower_quantified_path(child)?);
            }
            other => bail!("internal: unexpected match element {other:?}"),
        }
    }

    let start = start.ok_or_else(|| anyhow::anyhow!("pattern has no node"))?;

    if !has_quant {
        // Plain pattern: fold the `Hop` segments back into the ordinary `rels`
        // chain so the existing `segments: None` machinery handles it verbatim.
        let rels = segments
            .into_iter()
            .map(|s| match s {
                Segment::Hop(r, n) => Ok((r, n)),
                Segment::Quantified { .. } => {
                    bail!("internal: quantified segment without has_quant flag")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Pattern {
            path_var,
            start,
            rels,
            segments: None,
            restrictor,
            selector,
        });
    }

    // Quantified patterns can't yet bind a whole-path variable (the desugaring
    // discards intermediate nodes, so a reconstructed `Path` would be incomplete).
    if path_var.is_some() {
        bail!("a path variable over a quantified path pattern is not yet supported");
    }
    // A restrictor over a quantified group is deferred (DECISIONS D36): the
    // group desugars into separate fixed-length expansions, which can't share one
    // uniqueness scope, so honouring TRAIL/ACYCLIC/SIMPLE across the repetitions is
    // not yet possible. Reject rather than silently ignore the restrictor.
    if restrictor.is_some() {
        bail!(
            "a path restrictor (WALK/TRAIL/ACYCLIC/SIMPLE) over a quantified path \
             pattern ('((…)){{m,n}}') is not yet supported; apply it to a \
             variable-length relationship instead"
        );
    }
    // A selector over a quantified group is likewise deferred: shortest-path search
    // runs over a single variable-length relationship, not a desugared group.
    if selector.is_some() {
        bail!(
            "a path selector (ANY/ALL SHORTEST or SHORTEST k) over a quantified path \
             pattern ('((…)){{m,n}}') is not yet supported; apply it to a \
             variable-length relationship instead"
        );
    }
    Ok(Pattern {
        path_var: None,
        start,
        rels: Vec::new(),
        segments: Some(segments),
        restrictor: None,
        selector: None,
    })
}

/// Lower a `path_restrictor` keyword into its [`PathRestrictor`] variant.
fn lower_path_restrictor(pair: Pair<Rule>) -> Result<PathRestrictor> {
    let kw = only_child(pair)?;
    Ok(match kw.as_rule() {
        Rule::kw_walk => PathRestrictor::Walk,
        Rule::kw_trail => PathRestrictor::Trail,
        Rule::kw_acyclic => PathRestrictor::Acyclic,
        Rule::kw_simple => PathRestrictor::Simple,
        other => bail!("internal: unexpected path_restrictor child {other:?}"),
    })
}

/// Lower `MATCH [p =] shortestPath(pattern)` / `allShortestPaths(pattern)` — the
/// openCypher function spelling — onto the same AST as the GQL `ANY SHORTEST` /
/// `ALL SHORTEST` selector: take the inner single-relationship pattern, attach the
/// selector and (optional) bound path variable, and let the executor's existing
/// shortest-path machinery (`apply_match_selected`) run it. `shortestPath` yields one
/// shortest path (AnyShortest); `allShortestPaths` yields every shortest path.
fn lower_shortest_call(pair: Pair<Rule>) -> Result<Pattern> {
    let mut path_var = None;
    let mut selector = None;
    let mut inner: Option<Pattern> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::path_var => path_var = Some(ident_text(child)?),
            Rule::shortest_kind => selector = Some(lower_shortest_kind(child)?),
            Rule::pattern => inner = Some(lower_pattern(child)?),
            other => bail!("internal: unexpected shortest_call child {other:?}"),
        }
    }
    let mut pat = inner.ok_or_else(|| anyhow::anyhow!("shortestPath() requires a pattern"))?;
    if pat.path_var.is_some() {
        bail!("the path variable goes before shortestPath(...), not inside its pattern");
    }
    pat.path_var = path_var;
    pat.selector = selector;
    Ok(pat)
}

/// Map the `shortestPath` / `allShortestPaths` function keyword to its selector.
fn lower_shortest_kind(pair: Pair<Rule>) -> Result<PathSelector> {
    let inner = only_child(pair)?;
    Ok(match inner.as_rule() {
        Rule::kw_shortestpath => PathSelector::AnyShortest,
        Rule::kw_allshortestpaths => PathSelector::AllShortest,
        other => bail!("internal: unexpected shortest_kind child {other:?}"),
    })
}

/// Lower a `path_selector` (`ANY SHORTEST` / `ALL SHORTEST` / `SHORTEST k`) into its
/// [`PathSelector`] variant.
fn lower_path_selector(pair: Pair<Rule>) -> Result<PathSelector> {
    let inner = only_child(pair)?;
    Ok(match inner.as_rule() {
        Rule::any_shortest => PathSelector::AnyShortest,
        Rule::all_shortest => PathSelector::AllShortest,
        Rule::shortest_k => {
            // `SHORTEST k`: skip the `kw_shortest` keyword child and read the count.
            let n = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::integer)
                .ok_or_else(|| anyhow::anyhow!("internal: SHORTEST k missing its count"))?;
            let k: u32 = n.as_str().parse().map_err(|_| {
                anyhow::anyhow!("SHORTEST count '{}' is not a valid integer", n.as_str())
            })?;
            if k == 0 {
                bail!("SHORTEST k requires a positive count (got 0)");
            }
            PathSelector::ShortestK(k)
        }
        other => bail!("internal: unexpected path_selector child {other:?}"),
    })
}

/// Lower `quantified_path = "(" quantified_inner ")" quantifier_bounds` into the
/// inner relationship chain plus its repetition bounds. The inner sub-path's
/// leading node juxtaposes with the preceding element's node, so labels/properties
/// on it would have to be enforced at every junction — not yet supported, so they
/// are rejected here rather than silently dropped.
fn lower_quantified_path(pair: Pair<Rule>) -> Result<(Vec<(RelPat, NodePat)>, VarLength)> {
    let mut inner: Vec<(RelPat, NodePat)> = Vec::new();
    let mut bounds: Option<VarLength> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::quantified_inner => {
                let mut nodes = Vec::new();
                let mut rels = Vec::new();
                for d in child.into_inner() {
                    match d.as_rule() {
                        Rule::node_pattern => nodes.push(lower_node_pattern(d)?),
                        Rule::rel_pattern => rels.push(lower_rel_pattern(d)?),
                        other => bail!("internal: unexpected quantified_inner child {other:?}"),
                    }
                }
                let mut nodes = nodes.into_iter();
                let first = nodes
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("quantified group has no node"))?;
                if first.label_expr.is_some() || !first.props.is_empty() {
                    bail!(
                        "labels or properties on the first node of a quantified path \
                         group are not yet supported"
                    );
                }
                for (rel, node) in rels.into_iter().zip(nodes) {
                    inner.push((rel, node));
                }
            }
            Rule::quantifier_bounds => bounds = Some(lower_quantifier_bounds(child)?),
            other => bail!("internal: unexpected quantified_path child {other:?}"),
        }
    }
    let bounds = bounds.ok_or_else(|| anyhow::anyhow!("quantified group has no bounds"))?;
    Ok((inner, bounds))
}

/// Lower `quantifier_bounds` (`{m}` / `{m,n}` / `{m,}` / `{,n}` / `+` / `*`) into a
/// [`VarLength`]. `+` is `{1,}`, `*` is `{0,}`; an absent bound is `None` (open).
fn lower_quantifier_bounds(pair: Pair<Rule>) -> Result<VarLength> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::exact_bound => {
            let n = parse_u32(only_child(inner)?)?;
            Ok(VarLength {
                min: Some(n),
                max: Some(n),
            })
        }
        Rule::range_bound => {
            let mut min = None;
            let mut max = None;
            for d in inner.into_inner() {
                match d.as_rule() {
                    Rule::quant_lo => min = Some(parse_u32(only_child(d)?)?),
                    Rule::quant_hi => max = Some(parse_u32(only_child(d)?)?),
                    other => bail!("internal: unexpected range_bound child {other:?}"),
                }
            }
            Ok(VarLength { min, max })
        }
        Rule::plus_bound => Ok(VarLength {
            min: Some(1),
            max: None,
        }),
        Rule::star_bound => Ok(VarLength {
            min: Some(0),
            max: None,
        }),
        other => bail!("internal: unexpected quantifier_bounds child {other:?}"),
    }
}

/// Parse an `integer` token, honouring openCypher's three radices: `0x…` is hex,
/// a leading `0` before further octal digits is octal (`017` is 15, not 17), and
/// anything else is decimal. The sign is carried into `from_str_radix` rather
/// than negated afterwards, so `i64::MIN` round-trips.
fn parse_int_literal(s: &str) -> Result<i64> {
    let neg = s.starts_with('-');
    let digits = s.strip_prefix('-').unwrap_or(s);
    let (radix, body) = match digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        Some(hex) => (16, hex),
        None if digits.len() > 1 && digits.starts_with('0') => (8, &digits[1..]),
        None => (10, digits),
    };
    let signed = if neg {
        format!("-{body}")
    } else {
        body.to_string()
    };
    i64::from_str_radix(&signed, radix).map_err(|e| anyhow::anyhow!("bad integer {s:?}: {e}"))
}

/// Parse a non-negative `integer` token used as a quantifier bound. Shares
/// [`parse_int_literal`]'s radix rules, so `*{017}` means 15 hops, not 17.
fn parse_u32(pair: Pair<Rule>) -> Result<u32> {
    let s = pair.as_str().trim();
    parse_int_literal(s)
        .ok()
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid quantifier bound '{s}'"))
}

fn lower_node_pattern(pair: Pair<Rule>) -> Result<NodePat> {
    let mut var = None;
    let mut label_expr = None;
    let mut props = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::var => var = Some(ident_text(only_child(child)?)?),
            Rule::labels => label_expr = Some(lower_labels(child)?),
            Rule::properties => props = lower_properties(child)?,
            other => bail!("internal: unexpected node child {other:?}"),
        }
    }
    Ok(NodePat {
        var,
        label_expr,
        props,
    })
}

fn lower_rel_pattern(pair: Pair<Rule>) -> Result<RelPat> {
    let mut left = false;
    let mut right = false;
    let mut var = None;
    let mut type_expr = None;
    let mut var_length = None;
    let mut props = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::left_arrow => left = true,
            Rule::right_arrow => right = true,
            Rule::rel_detail => {
                for d in child.into_inner() {
                    match d.as_rule() {
                        Rule::var => var = Some(ident_text(only_child(d)?)?),
                        // `rel_types` and node `labels` share the `label_expr` grammar.
                        Rule::rel_types => type_expr = Some(lower_labels(d)?),
                        Rule::var_length => var_length = Some(lower_var_length(d)?),
                        Rule::properties => props = lower_properties(d)?,
                        other => bail!("internal: unexpected rel_detail child {other:?}"),
                    }
                }
            }
            other => bail!("internal: unexpected rel child {other:?}"),
        }
    }
    let dir = match (left, right) {
        (true, false) => Direction::Incoming,
        (false, true) => Direction::Outgoing,
        (false, false) => Direction::Undirected,
        (true, true) => bail!("a relationship cannot point in both directions"),
    };
    Ok(RelPat {
        var,
        dir,
        type_expr,
        var_length,
        props,
    })
}

fn lower_var_length(pair: Pair<Rule>) -> Result<VarLength> {
    // var_length = { "*" ~ range_spec? }; range_spec = { integer? ~ (".." ~ integer?)? }
    let Some(spec) = pair.into_inner().next() else {
        return Ok(VarLength {
            min: None,
            max: None,
        });
    };
    let text = spec.as_str();
    let ints: Vec<Pair<Rule>> = spec
        .into_inner()
        .filter(|p| p.as_rule() == Rule::integer)
        .collect();
    let parse_u32 = |p: &Pair<Rule>| -> Result<u32> {
        parse_int_literal(p.as_str())
            .ok()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| anyhow::anyhow!("bad var-length bound {:?}", p.as_str()))
    };
    if !text.contains("..") {
        // `*3` — exact length.
        let n = ints.first().map(parse_u32).transpose()?;
        Ok(VarLength { min: n, max: n })
    } else {
        // `*min..max`, either side optional. The integers map left-to-right onto
        // the present bounds depending on which side of `..` they sit.
        let (before, _after) = text.split_once("..").unwrap();
        let mut iter = ints.iter();
        let min = if before.trim().is_empty() {
            None
        } else {
            Some(parse_u32(iter.next().unwrap())?)
        };
        let max = iter.next().map(&parse_u32).transpose()?;
        Ok(VarLength { min, max })
    }
}

/// Lower a `labels` / `rel_types` pair (`":" ~ label_expr`) into a [`LabelExpr`].
/// Both node and relationship pattern elements share this grammar and AST.
fn lower_labels(pair: Pair<Rule>) -> Result<LabelExpr> {
    let expr = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::label_expr)
        .ok_or_else(|| anyhow::anyhow!("label expression has no body"))?;
    lower_label_expr(expr)
}

// The label-expression grammar mirrors the standard precedence climb: `label_expr`
// → OR over `le_and` → AND over `le_not` → leading `!`s over `le_atom` (a name or a
// parenthesised sub-expression). Each lowering peels one layer; absent operators
// collapse to the single operand, so `:A` lowers to a bare `Atom`.

fn lower_label_expr(pair: Pair<Rule>) -> Result<LabelExpr> {
    lower_le_or(only_child(pair)?)
}

fn lower_le_or(pair: Pair<Rule>) -> Result<LabelExpr> {
    let mut ands = pair.into_inner().filter(|p| p.as_rule() == Rule::le_and);
    let first = ands
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty label OR expression"))?;
    let mut acc = lower_le_and(first)?;
    for rhs in ands {
        acc = LabelExpr::Or(Box::new(acc), Box::new(lower_le_and(rhs)?));
    }
    Ok(acc)
}

fn lower_le_and(pair: Pair<Rule>) -> Result<LabelExpr> {
    let mut nots = pair.into_inner().filter(|p| p.as_rule() == Rule::le_not);
    let first = nots
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty label AND expression"))?;
    let mut acc = lower_le_not(first)?;
    for rhs in nots {
        acc = LabelExpr::And(Box::new(acc), Box::new(lower_le_not(rhs)?));
    }
    Ok(acc)
}

fn lower_le_not(pair: Pair<Rule>) -> Result<LabelExpr> {
    let mut bangs = 0usize;
    let mut atom = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::le_bang => bangs += 1,
            Rule::le_atom => atom = Some(lower_le_atom(child)?),
            other => bail!("internal: unexpected le_not child {other:?}"),
        }
    }
    let mut expr = atom.ok_or_else(|| anyhow::anyhow!("label NOT without an atom"))?;
    for _ in 0..bangs {
        expr = LabelExpr::Not(Box::new(expr));
    }
    Ok(expr)
}

fn lower_le_atom(pair: Pair<Rule>) -> Result<LabelExpr> {
    let child = only_child(pair)?;
    match child.as_rule() {
        Rule::le_name => Ok(LabelExpr::Atom(ident_text(child)?)),
        Rule::label_expr => lower_label_expr(child),
        other => bail!("internal: unexpected le_atom child {other:?}"),
    }
}

/// `Properties = MapLiteral | Parameter`. Both spellings parse; only the inline
/// map lowers, because matching a pattern against a map supplied at runtime needs
/// the executor to build the predicate per row, which it cannot yet do.
fn lower_properties(pair: Pair<Rule>) -> Result<Vec<(String, Expr)>> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::prop_map => lower_prop_map(inner),
        Rule::parameter => bail!(
            "a parameter property map in a pattern is not supported; \
             write the properties inline, e.g. (n:Label {{key: $value}})"
        ),
        other => bail!("internal: unexpected properties child {other:?}"),
    }
}

fn lower_prop_map(pair: Pair<Rule>) -> Result<Vec<(String, Expr)>> {
    let mut out = Vec::new();
    for entry in pair.into_inner() {
        if entry.as_rule() != Rule::prop_entry {
            continue;
        }
        let mut it = entry.into_inner();
        let key = ident_text(it.next().unwrap())?;
        let val = lower_expr(it.next().unwrap())?;
        out.push((key, val));
    }
    Ok(out)
}

// ── Expression lowering (precedence already encoded by the grammar) ──────────

fn lower_expr(pair: Pair<Rule>) -> Result<Expr> {
    match pair.as_rule() {
        Rule::expr => lower_expr(only_child(pair)?),
        Rule::or_expr => fold_logical(pair, Expr::Or),
        Rule::and_expr => fold_logical(pair, Expr::And),
        Rule::xor_expr => fold_logical(pair, Expr::Xor),
        Rule::not_expr => lower_not(pair),
        Rule::comparison => lower_comparison(pair),
        Rule::add_expr => lower_arith(pair),
        Rule::mul_expr => lower_arith(pair),
        Rule::pow_expr => lower_arith(pair),
        Rule::unary_expr => lower_unary(pair),
        Rule::postfix_expr => lower_postfix(pair),
        other => bail!("internal: lower_expr on {other:?}"),
    }
}

/// For `or_expr`/`and_expr`/`xor_expr`: one child passes through; many fold.
/// (The connecting keyword tokens are filtered out by `kids`.)
fn fold_logical(pair: Pair<Rule>, build: impl Fn(Vec<Expr>) -> Expr) -> Result<Expr> {
    let parts: Vec<Expr> = kids(pair).map(lower_expr).collect::<Result<_>>()?;
    Ok(if parts.len() == 1 {
        parts.into_iter().next().unwrap()
    } else {
        build(parts)
    })
}

fn lower_not(pair: Pair<Rule>) -> Result<Expr> {
    let mut nots = 0;
    let mut inner = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::not_op => nots += 1,
            Rule::comparison => inner = Some(lower_comparison(child)?),
            other => bail!("internal: unexpected not_expr child {other:?}"),
        }
    }
    let mut e = inner.ok_or_else(|| anyhow::anyhow!("NOT without operand"))?;
    for _ in 0..nots {
        e = Expr::Not(Box::new(e));
    }
    Ok(e)
}

fn lower_comparison(pair: Pair<Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let mut left = lower_expr(inner.next().unwrap())?;
    for rhs in inner {
        // comp_rhs = { string_op add | in_op add | null_test | comp_op add }
        let mut parts = rhs.into_inner();
        let op_pair = parts.next().unwrap();
        match op_pair.as_rule() {
            Rule::comp_op => {
                let right = lower_expr(parts.next().unwrap())?;
                // `=~` is the regex full-match operator, not an equality comparison.
                left = if op_pair.as_str() == "=~" {
                    Expr::StringOp(StrOp::Regex, Box::new(left), Box::new(right))
                } else {
                    Expr::Compare(cmp_op(op_pair.as_str()), Box::new(left), Box::new(right))
                };
            }
            Rule::string_op => {
                let right = lower_expr(parts.next().unwrap())?;
                left = Expr::StringOp(str_op(op_pair.as_str()), Box::new(left), Box::new(right));
            }
            Rule::in_op => {
                let right = lower_expr(parts.next().unwrap())?;
                left = Expr::In(Box::new(left), Box::new(right));
            }
            Rule::null_test => {
                let negated = op_pair.as_str().to_lowercase().contains("not");
                left = Expr::IsNull(Box::new(left), negated);
            }
            other => bail!("internal: unexpected comp_rhs op {other:?}"),
        }
    }
    Ok(left)
}

fn lower_arith(pair: Pair<Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let mut left = lower_expr(inner.next().unwrap())?;
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "+" => BinOp::Add,
            "-" => BinOp::Sub,
            "*" => BinOp::Mul,
            "/" => BinOp::Div,
            "%" => BinOp::Mod,
            "^" => BinOp::Pow,
            other => bail!("internal: bad arith op {other:?}"),
        };
        let right = lower_expr(inner.next().unwrap())?;
        left = Expr::Arith(op, Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn lower_unary(pair: Pair<Rule>) -> Result<Expr> {
    let mut negs = 0;
    let mut inner = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::sign => {
                if child.as_str() == "-" {
                    negs += 1;
                }
            }
            Rule::postfix_expr => inner = Some(lower_postfix(child)?),
            other => bail!("internal: unexpected unary child {other:?}"),
        }
    }
    let mut e = inner.ok_or_else(|| anyhow::anyhow!("unary without operand"))?;
    if negs % 2 == 1 {
        e = Expr::Neg(Box::new(e));
    }
    Ok(e)
}

fn lower_postfix(pair: Pair<Rule>) -> Result<Expr> {
    let mut inner = pair.into_inner();
    let mut base = lower_primary(inner.next().unwrap())?;
    for op in inner {
        let inner_op = only_child(op)?;
        match inner_op.as_rule() {
            Rule::property_access => {
                let key = ident_text(only_child(inner_op)?)?;
                base = Expr::Property(Box::new(base), key);
            }
            Rule::label_pred => {
                let labels: Vec<String> = inner_op
                    .into_inner()
                    .filter(|p| p.as_rule() == Rule::label_name)
                    .map(ident_text)
                    .collect::<Result<_>>()?;
                base = Expr::HasLabels(Box::new(base), labels);
            }
            Rule::slice_access => {
                let mut from = None;
                let mut to = None;
                for part in inner_op.into_inner() {
                    match part.as_rule() {
                        // `slice_from`/`slice_to` wrap an optional `expr`; an
                        // absent bound leaves the option `None` (open end).
                        Rule::slice_from => {
                            if let Some(e) = part.into_inner().next() {
                                from = Some(Box::new(lower_expr(e)?));
                            }
                        }
                        Rule::slice_to => {
                            if let Some(e) = part.into_inner().next() {
                                to = Some(Box::new(lower_expr(e)?));
                            }
                        }
                        other => bail!("internal: unexpected slice part {other:?}"),
                    }
                }
                base = Expr::Slice {
                    base: Box::new(base),
                    from,
                    to,
                };
            }
            Rule::index_access => {
                let idx = lower_expr(only_child(inner_op)?)?;
                base = Expr::Index(Box::new(base), Box::new(idx));
            }
            other => bail!("internal: unexpected postfix op {other:?}"),
        }
    }
    Ok(base)
}

fn lower_primary(pair: Pair<Rule>) -> Result<Expr> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::literal => lower_literal(inner),
        Rule::parameter => Ok(Expr::Param(ident_text(only_child(inner)?)?)),
        Rule::variable => Ok(Expr::Var(ident_text(only_child(inner)?)?)),
        Rule::parens => lower_expr(only_child(inner)?),
        Rule::list_literal => {
            let items = inner.into_inner().map(lower_expr).collect::<Result<_>>()?;
            Ok(Expr::List(items))
        }
        Rule::map_literal => {
            let mut entries = Vec::new();
            for e in inner.into_inner() {
                let mut it = e.into_inner();
                let key = ident_text(it.next().unwrap())?;
                let val = lower_expr(it.next().unwrap())?;
                entries.push((key, val));
            }
            Ok(Expr::Map(entries))
        }
        Rule::function_call => lower_function(inner),
        Rule::cast_expr => lower_cast(inner),
        Rule::case_expr => lower_case(inner),
        Rule::map_projection => lower_map_projection(inner),
        Rule::reduce_expr => lower_reduce(inner),
        Rule::list_comprehension => lower_list_predicate(inner),
        Rule::list_comp => lower_list_comp(inner),
        Rule::pattern_comp => lower_pattern_comp(inner),
        Rule::pattern_predicate => lower_pattern_predicate(inner),
        Rule::exists_subquery => lower_exists(inner),
        Rule::shortest_path => lower_shortest_path(inner),
        other => bail!("internal: unexpected primary {other:?}"),
    }
}

fn lower_function(pair: Pair<Rule>) -> Result<Expr> {
    let distinct = has_keyword(&pair, "distinct");
    let mut iter = pair.into_inner();
    let name = ident_text(iter.next().unwrap())?;
    let mut args = Vec::new();
    let mut star = false;
    for a in iter {
        if a.as_rule() != Rule::func_arg {
            continue;
        }
        let inner = only_child(a)?;
        match inner.as_rule() {
            Rule::star_arg => star = true,
            _ => args.push(lower_expr(inner)?),
        }
    }
    Ok(Expr::Function {
        name,
        distinct,
        args: if star {
            FuncArgs::Star
        } else {
            FuncArgs::Args(args)
        },
    })
}

fn lower_cast(pair: Pair<Rule>) -> Result<Expr> {
    // cast_expr = { kw_cast ~ "(" ~ expr ~ kw_as ~ cast_type ~ ")" }. kids() drops
    // the keyword tokens, leaving the value expression then the target type name.
    // GQL's CAST is a typed-value conversion; we lower it onto slater's existing
    // conversion functions so the executor path is shared (no new coercion code) —
    // the same additive discipline as FOR → UNWIND (DECISIONS D42).
    let mut it = kids(pair);
    let value = lower_expr(
        it.next()
            .ok_or_else(|| anyhow::anyhow!("CAST without value"))?,
    )?;
    let ty = it
        .next()
        .ok_or_else(|| anyhow::anyhow!("CAST without target type"))?;
    let func = match ty.as_str().to_ascii_lowercase().as_str() {
        "integer" | "int" => "toInteger",
        "float" | "double" | "real" => "toFloat",
        "string" | "varchar" => "toString",
        "boolean" | "bool" => "toBoolean",
        // The temporal types map to their single-argument constructors, which already
        // accept a string/map and yield the temporal Val.
        "date" => "date",
        "localdatetime" => "localdatetime",
        "localtime" => "localtime",
        "duration" => "duration",
        other => bail!("internal: unexpected CAST type {other:?}"),
    };
    Ok(Expr::Function {
        name: func.to_string(),
        distinct: false,
        args: FuncArgs::Args(vec![value]),
    })
}

fn lower_case(pair: Pair<Rule>) -> Result<Expr> {
    let mut subject = None;
    let mut whens = Vec::new();
    let mut els = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::when_clause => {
                let mut it = kids(child);
                let cond = lower_expr(it.next().unwrap())?;
                let then = lower_expr(it.next().unwrap())?;
                whens.push((cond, then));
            }
            Rule::else_clause => els = Some(Box::new(lower_expr(only_child(child)?)?)),
            Rule::case_subject => subject = Some(Box::new(lower_expr(only_child(child)?)?)),
            other => bail!("internal: unexpected case child {other:?}"),
        }
    }
    Ok(Expr::Case {
        subject,
        whens,
        els,
    })
}

fn lower_map_projection(pair: Pair<Rule>) -> Result<Expr> {
    let mut iter = pair.into_inner();
    let var = ident_text(iter.next().unwrap())?;
    let mut items = Vec::new();
    for item in iter {
        let inner = only_child(item)?;
        match inner.as_rule() {
            Rule::proj_all => items.push(MapProjItem::AllProps),
            Rule::proj_property => {
                items.push(MapProjItem::Property(ident_text(only_child(inner)?)?))
            }
            Rule::proj_literal => {
                let mut it = inner.into_inner();
                let key = ident_text(it.next().unwrap())?;
                let val = lower_expr(it.next().unwrap())?;
                items.push(MapProjItem::Literal(key, val));
            }
            other => bail!("internal: unexpected map projection item {other:?}"),
        }
    }
    Ok(Expr::MapProjection { var, items })
}

fn lower_list_predicate(pair: Pair<Rule>) -> Result<Expr> {
    let mut quant = None;
    let mut var = None;
    let mut list = None;
    let mut predicate = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::quantifier => {
                quant = Some(match child.as_str().to_lowercase().as_str() {
                    "any" => Quantifier::Any,
                    "all" => Quantifier::All,
                    "none" => Quantifier::None,
                    "single" => Quantifier::Single,
                    other => bail!("internal: bad quantifier {other:?}"),
                })
            }
            Rule::identifier => var = Some(ident_text(child)?),
            Rule::expr => list = Some(Box::new(lower_expr(child)?)),
            Rule::where_clause => predicate = Some(Box::new(lower_expr(only_child(child)?)?)),
            other => bail!("internal: unexpected list-pred child {other:?}"),
        }
    }
    Ok(Expr::ListPredicate {
        quant: quant.ok_or_else(|| anyhow::anyhow!("missing quantifier"))?,
        var: var.ok_or_else(|| anyhow::anyhow!("missing list-pred variable"))?,
        list: list.ok_or_else(|| anyhow::anyhow!("missing list-pred list"))?,
        predicate,
    })
}

fn lower_list_comp(pair: Pair<Rule>) -> Result<Expr> {
    // list_comp = { "[" ~ identifier ~ kw_in ~ expr ~ ((where_clause ~ ("|" expr)?) | ("|" expr)) ~ "]" }
    // After kids() drops kw_in, the children appear in source order: the iteration
    // identifier, the source-list `expr`, an optional `where_clause`, then an
    // optional projection `expr`. The first `expr` is always the list; a second
    // `expr` is the projection.
    let mut var = None;
    let mut list = None;
    let mut predicate = None;
    let mut projection = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::identifier => var = Some(ident_text(child)?),
            Rule::where_clause => predicate = Some(Box::new(lower_expr(only_child(child)?)?)),
            Rule::expr => {
                let e = Box::new(lower_expr(child)?);
                if list.is_none() {
                    list = Some(e);
                } else {
                    projection = Some(e);
                }
            }
            other => bail!("internal: unexpected list_comp child {other:?}"),
        }
    }
    Ok(Expr::ListComprehension {
        var: var.ok_or_else(|| anyhow::anyhow!("missing list-comprehension variable"))?,
        list: list.ok_or_else(|| anyhow::anyhow!("missing list-comprehension list"))?,
        predicate,
        projection,
    })
}

fn lower_reduce(pair: Pair<Rule>) -> Result<Expr> {
    // reduce_expr = { reduce_kw ~ "(" ~ identifier ~ "=" ~ expr ~ "," ~
    //                 identifier ~ kw_in ~ expr ~ "|" ~ expr ~ ")" }
    // After kids() drops `reduce_kw`/`kw_in`, the children appear in source order:
    // acc-var identifier, acc-init expr, loop-var identifier, list expr, body expr.
    let mut acc_var = None;
    let mut var = None;
    let mut exprs: Vec<Expr> = Vec::new();
    for child in kids(pair) {
        match child.as_rule() {
            Rule::identifier => {
                let name = ident_text(child)?;
                if acc_var.is_none() {
                    acc_var = Some(name);
                } else {
                    var = Some(name);
                }
            }
            Rule::expr => exprs.push(lower_expr(child)?),
            other => bail!("internal: unexpected reduce child {other:?}"),
        }
    }
    if exprs.len() != 3 {
        bail!(
            "internal: reduce expects 3 sub-expressions, got {}",
            exprs.len()
        );
    }
    let mut it = exprs.into_iter();
    Ok(Expr::Reduce {
        acc_var: acc_var.ok_or_else(|| anyhow::anyhow!("missing reduce accumulator"))?,
        acc_init: Box::new(it.next().unwrap()),
        var: var.ok_or_else(|| anyhow::anyhow!("missing reduce variable"))?,
        list: Box::new(it.next().unwrap()),
        body: Box::new(it.next().unwrap()),
    })
}

fn lower_pattern_comp(pair: Pair<Rule>) -> Result<Expr> {
    // pattern_comp = { "[" ~ pattern ~ where_clause? ~ "|" ~ expr ~ "]" }
    let mut pattern = None;
    let mut predicate = None;
    let mut projection = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::pattern => pattern = Some(Box::new(lower_pattern(child)?)),
            Rule::where_clause => predicate = Some(Box::new(lower_expr(only_child(child)?)?)),
            Rule::expr => projection = Some(Box::new(lower_expr(child)?)),
            other => bail!("internal: unexpected pattern_comp child {other:?}"),
        }
    }
    Ok(Expr::PatternComprehension {
        pattern: pattern.ok_or_else(|| anyhow::anyhow!("missing pattern-comprehension pattern"))?,
        predicate,
        projection: projection
            .ok_or_else(|| anyhow::anyhow!("missing pattern-comprehension projection"))?,
    })
}

fn lower_pattern_predicate(pair: Pair<Rule>) -> Result<Expr> {
    // pattern_predicate = { node_pattern ~ (rel_pattern ~ node_pattern)+ }
    let mut nodes = Vec::new();
    let mut rels = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::node_pattern => nodes.push(lower_node_pattern(child)?),
            Rule::rel_pattern => rels.push(lower_rel_pattern(child)?),
            other => bail!("internal: unexpected pattern_predicate child {other:?}"),
        }
    }
    let mut nodes = nodes.into_iter();
    let start = nodes
        .next()
        .ok_or_else(|| anyhow::anyhow!("pattern predicate has no node"))?;
    let chain: Vec<(RelPat, NodePat)> = rels.into_iter().zip(nodes).collect();
    Ok(Expr::PatternPredicate(Box::new(Pattern {
        path_var: None,
        start,
        rels: chain,
        segments: None,
        restrictor: None,
        selector: None,
    })))
}

fn lower_exists(pair: Pair<Rule>) -> Result<Expr> {
    // exists_subquery = { exists_kw ~ "{" ~ kw_match? ~ pattern ~ ("," ~ pattern)*
    //                     ~ where_clause? ~ "}" }
    let mut patterns = Vec::new();
    let mut predicate = None;
    for child in kids(pair) {
        match child.as_rule() {
            Rule::pattern => patterns.push(lower_pattern(child)?),
            Rule::where_clause => predicate = Some(Box::new(lower_expr(only_child(child)?)?)),
            // `EXISTS { RegularQuery }` — the reference grammar's other arm. It
            // parses so we can name the limitation instead of emitting a syntax error.
            Rule::subquery => bail!(
                "only the pattern form of EXISTS {{ … }} is supported; \
                 a RETURN-bearing subquery must be spelled CALL {{ … }}"
            ),
            other => bail!("internal: unexpected exists child {other:?}"),
        }
    }
    if patterns.is_empty() {
        bail!("EXISTS subquery has no pattern");
    }
    Ok(Expr::Exists {
        patterns,
        predicate,
    })
}

fn lower_shortest_path(pair: Pair<Rule>) -> Result<Expr> {
    // shortest_path = { shortest_path_kw ~ "(" ~ pattern ~ ")" }
    let pat = pair
        .into_inner()
        .find(|c| c.as_rule() == Rule::pattern)
        .ok_or_else(|| anyhow::anyhow!("shortestPath() requires a pattern"))?;
    Ok(Expr::ShortestPath(Box::new(lower_pattern(pat)?)))
}

fn lower_literal(pair: Pair<Rule>) -> Result<Expr> {
    let inner = only_child(pair)?;
    let v = match inner.as_rule() {
        Rule::integer => Value::Int(parse_int_literal(inner.as_str())?),
        Rule::float => Value::Float(
            inner
                .as_str()
                .parse::<f64>()
                .map_err(|e| anyhow::anyhow!("bad float {:?}: {e}", inner.as_str()))?,
        ),
        Rule::boolean => Value::Bool(inner.as_str().eq_ignore_ascii_case("true")),
        Rule::null => Value::Null,
        Rule::string => Value::Str(unescape_string(inner)?),
        other => bail!("internal: unexpected literal {other:?}"),
    };
    Ok(Expr::Literal(v))
}

// ── Small helpers ────────────────────────────────────────────────────────────

/// Keyword rules are atomic (so their word-boundary check is not broken by
/// implicit whitespace), which means they surface as leaf tokens in the parse
/// tree. They carry no AST data, so lowering filters them out.
fn is_kw(r: Rule) -> bool {
    matches!(
        r,
        Rule::kw_union
            | Rule::kw_all
            | Rule::kw_optional
            | Rule::kw_match
            | Rule::kw_where
            | Rule::kw_return
            | Rule::kw_with
            | Rule::kw_distinct
            | Rule::kw_as
            | Rule::kw_order
            | Rule::kw_by
            | Rule::kw_skip
            | Rule::kw_limit
            | Rule::kw_asc
            | Rule::kw_desc
            | Rule::kw_or
            | Rule::kw_and
            | Rule::kw_xor
            | Rule::kw_not
            | Rule::kw_in
            | Rule::kw_is
            | Rule::kw_null
            | Rule::kw_true
            | Rule::kw_false
            | Rule::kw_starts
            | Rule::kw_ends
            | Rule::kw_contains
            | Rule::kw_case
            | Rule::kw_when
            | Rule::kw_then
            | Rule::kw_else
            | Rule::kw_end
            | Rule::kw_set
            | Rule::kw_remove
            | Rule::kw_call
            | Rule::kw_yield
            | Rule::kw_unwind
            | Rule::kw_for
            | Rule::kw_cast
            | Rule::reduce_kw
            | Rule::exists_kw
    )
}

/// A pair's child rules with the atomic keyword tokens filtered out.
fn kids(pair: Pair<Rule>) -> impl Iterator<Item = Pair<Rule>> {
    pair.into_inner().filter(|p| !is_kw(p.as_rule()))
}

fn only_child(pair: Pair<Rule>) -> Result<Pair<Rule>> {
    kids(pair)
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected a child rule"))
}

/// Whether `kw` appears as a standalone keyword in the (lowercased) matched text
/// of `pair` — used for `DISTINCT` flags that the grammar does not capture.
fn has_keyword(pair: &Pair<Rule>, kw: &str) -> bool {
    pair.as_str()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|w| w.eq_ignore_ascii_case(kw))
}

/// Extract an identifier's text, unquoting a backtick-escaped name if present.
fn ident_text(pair: Pair<Rule>) -> Result<String> {
    // `var`/`label_name`/`alias` wrap an `identifier`; unwrap one layer if present.
    let p = if pair.as_rule() == Rule::identifier {
        pair
    } else {
        match pair.clone().into_inner().next() {
            Some(c) if c.as_rule() == Rule::identifier => c,
            _ => pair,
        }
    };
    Ok(unquote_name(p.as_str()))
}

/// Strip the outer backticks of an `EscapedSymbolicName` and collapse each
/// doubled backtick to one, so `` `a``b` `` names the single identifier ``a`b``.
/// A bare (unquoted) name is returned verbatim — it can never start with a backtick.
fn unquote_name(s: &str) -> String {
    match s.strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
        Some(inner) => inner.replace("``", "`"),
        None => s.to_string(),
    }
}

fn unescape_string(pair: Pair<Rule>) -> Result<String> {
    // string = ${ "'" ~ sq_inner ~ "'" | "\"" ~ dq_inner ~ "\"" }
    let inner = only_child(pair)?;
    let raw = inner.as_str();
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        // Recognised escapes mirror libcypher-parser's `escaped-char` rule; any
        // other char keeps its backslash (so e.g. a regex `\w` survives intact,
        // matching FalkorDB rather than collapsing to `w`).
        match chars.next() {
            Some('a') => out.push('\u{07}'),
            Some('b') => out.push('\u{08}'),
            Some('f') => out.push('\u{0C}'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('v') => out.push('\u{0B}'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('"') => out.push('"'),
            Some('?') => out.push('?'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    Ok(out)
}

fn cmp_op(s: &str) -> CmpOp {
    match s {
        "=" => CmpOp::Eq,
        "<>" => CmpOp::Ne,
        "<" => CmpOp::Lt,
        "<=" => CmpOp::Le,
        ">" => CmpOp::Gt,
        ">=" => CmpOp::Ge,
        _ => CmpOp::Eq,
    }
}

fn str_op(s: &str) -> StrOp {
    let l = s.to_lowercase();
    if l.contains("starts") {
        StrOp::StartsWith
    } else if l.contains("ends") {
        StrOp::EndsWith
    } else {
        StrOp::Contains
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(q: &str) -> Query {
        parse(q).unwrap_or_else(|e| panic!("expected accept for {q:?}: {e}"))
    }

    fn err(q: &str) -> String {
        match parse(q) {
            Ok(_) => panic!("expected reject for {q:?}"),
            Err(e) => e.to_string(),
        }
    }

    fn write(q: &str) -> WriteStmt {
        match parse_statement(q).unwrap_or_else(|e| panic!("expected accept for {q:?}: {e}")) {
            Statement::Write(w) => w,
            other => panic!("expected a node write for {q:?}, got {other:?}"),
        }
    }

    fn edge_write(q: &str) -> EdgeWriteStmt {
        match parse_statement(q).unwrap_or_else(|e| panic!("expected accept for {q:?}: {e}")) {
            Statement::WriteEdge(w) => w,
            other => panic!("expected an edge write for {q:?}, got {other:?}"),
        }
    }

    fn write_err(q: &str) -> String {
        match parse_statement(q) {
            Ok(
                Statement::Write(_)
                | Statement::Create(_)
                | Statement::WriteEdge(_)
                | Statement::Consolidate,
            ) => {
                panic!("expected reject for {q:?}")
            }
            // An unsupported write shape falls through to the read parser, which
            // rejects it; either way `parse_statement` must not yield a Write.
            Ok(Statement::Read(_)) => "read-only".to_string(),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn write_statement_lowers_business_key_set() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) SET n.price = 10, n.sector = 'Tech'");
        assert_eq!(w.var, "n");
        assert_eq!(w.label, "Company");
        assert_eq!(w.key, "ticker");
        assert_eq!(w.key_value, Expr::Literal(Value::Str("ABC".into())));
        assert_eq!(
            w.op,
            WriteOp::Set(vec![
                SetItem::Prop {
                    prop: "price".to_string(),
                    value: Expr::Literal(Value::Int(10)),
                },
                SetItem::Prop {
                    prop: "sector".to_string(),
                    value: Expr::Literal(Value::Str("Tech".into())),
                },
            ])
        );
        assert!(w.ret.is_none());
    }

    #[test]
    fn write_unwind_lowers_a_batched_node_write() {
        let r = |field: &str| Expr::Property(Box::new(Expr::Var("r".into())), field.into());

        let w = write("UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age");
        assert!(w.upsert, "MERGE anchor");
        let (src, var) = w.unwind.clone().expect("unwind captured");
        assert_eq!(src, Expr::Param("rows".into()));
        assert_eq!(var, "r");
        assert_eq!(w.label, "Person");
        assert_eq!(w.key, "name");
        assert_eq!(
            w.key_value,
            r("name"),
            "the business key reads the row field"
        );
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::Prop {
                prop: "age".to_string(),
                value: r("age"),
            }]),
            "SET values read row fields"
        );

        // A batched DELETE lowers too (MATCH anchor, DELETE the row's node).
        let d = write("UNWIND $rows AS r MATCH (n:Person {name: r.name}) DELETE n");
        assert!(d.unwind.is_some());
        assert!(!d.upsert);
        assert_eq!(
            d.op,
            WriteOp::Delete {
                detach: false,
                targets: vec!["n".to_string()]
            }
        );

        // A plain (non-UNWIND) write carries no unwind, and still rejects non-constants.
        assert!(write("MERGE (n:Person {name: 'Zoe'}) SET n.age = 1")
            .unwind
            .is_none());
        assert!(
            write_err("MATCH (n:Person {name: 'Zoe'}) SET n.age = n.x").contains("literal")
                || write_err("MATCH (n:Person {name: 'Zoe'}) SET n.age = n.x") == "read-only",
            "a plain write still forbids computed values"
        );
    }

    #[test]
    fn write_statement_accepts_params_and_return() {
        let w = write("MATCH (c:Company {ticker: $t}) SET c.price = $p RETURN c");
        assert_eq!(w.key_value, Expr::Param("t".into()));
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::Prop {
                prop: "price".to_string(),
                value: Expr::Param("p".into()),
            }])
        );
        assert!(w.ret.is_some());
    }

    #[test]
    fn write_statement_lowers_delete() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) DELETE n");
        assert_eq!(w.var, "n");
        assert_eq!(w.label, "Company");
        assert_eq!(w.key, "ticker");
        assert_eq!(
            w.op,
            WriteOp::Delete {
                detach: false,
                targets: vec!["n".to_string()]
            }
        );
        assert!(w.ret.is_none());

        let d = write("MATCH (n:Company {ticker: 'ABC'}) DETACH DELETE n");
        assert_eq!(
            d.op,
            WriteOp::Delete {
                detach: true,
                targets: vec!["n".to_string()]
            }
        );

        // DELETE must name the anchor variable.
        let e = write_err("MATCH (n:Company {ticker: 'ABC'}) DELETE m");
        assert!(
            e.contains("DELETE must target") || e == "read-only",
            "got: {e}"
        );
    }

    // ── Stage 1: widened SET / REMOVE / DELETE (parse-only) ─────────────────────

    #[test]
    fn set_replace_map_lowers() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) SET n = {price: 10, sector: 'Tech'}");
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::ReplaceMap(vec![
                ("price".to_string(), Expr::Literal(Value::Int(10))),
                (
                    "sector".to_string(),
                    Expr::Literal(Value::Str("Tech".into()))
                ),
            ])])
        );
    }

    #[test]
    fn set_merge_map_lowers() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) SET n += {price: 10}");
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::MergeMap(vec![(
                "price".to_string(),
                Expr::Literal(Value::Int(10))
            )])])
        );
    }

    #[test]
    fn set_labels_lowers() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) SET n:Listed:Tech");
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::AddLabels(vec![
                "Listed".to_string(),
                "Tech".to_string()
            ])])
        );
    }

    #[test]
    fn set_items_mix_shapes_in_source_order() {
        let w = write(
            "MATCH (n:Company {ticker: 'ABC'}) SET n.price = 10, n += {sector: 'Tech'}, n:Listed",
        );
        assert_eq!(
            w.op,
            WriteOp::Set(vec![
                SetItem::Prop {
                    prop: "price".to_string(),
                    value: Expr::Literal(Value::Int(10)),
                },
                SetItem::MergeMap(vec![(
                    "sector".to_string(),
                    Expr::Literal(Value::Str("Tech".into()))
                )]),
                SetItem::AddLabels(vec!["Listed".to_string()]),
            ])
        );
    }

    #[test]
    fn remove_prop_lowers() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) REMOVE n.price");
        assert_eq!(
            w.op,
            WriteOp::Remove(vec![RemoveItem::Prop("price".to_string())])
        );
    }

    #[test]
    fn remove_labels_lowers() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) REMOVE n:Listed:Tech");
        assert_eq!(
            w.op,
            WriteOp::Remove(vec![RemoveItem::Labels(vec![
                "Listed".to_string(),
                "Tech".to_string()
            ])])
        );
    }

    #[test]
    fn remove_items_mix_prop_and_labels() {
        let w = write("MATCH (n:Company {ticker: 'ABC'}) REMOVE n.price, n:Listed");
        assert_eq!(
            w.op,
            WriteOp::Remove(vec![
                RemoveItem::Prop("price".to_string()),
                RemoveItem::Labels(vec!["Listed".to_string()]),
            ])
        );
    }

    #[test]
    fn multi_target_delete_lowers() {
        // In the anchored model every target is the anchor variable; the grammar
        // still parses the comma-separated form.
        let w = write("MATCH (n:Company {ticker: 'ABC'}) DELETE n, n");
        assert_eq!(
            w.op,
            WriteOp::Delete {
                detach: false,
                targets: vec!["n".to_string(), "n".to_string()],
            }
        );
    }

    #[test]
    fn widened_write_items_target_the_anchor() {
        // SET / REMOVE items pointing at a non-anchor variable are rejected by name.
        assert!(write_err("MATCH (n:Company {ticker: 'A'}) SET m += {x: 1}")
            .contains("anchor variable"));
        assert!(
            write_err("MATCH (n:Company {ticker: 'A'}) SET m:Listed").contains("anchor variable")
        );
        assert!(write_err("MATCH (n:Company {ticker: 'A'}) REMOVE m.x").contains("anchor variable"));
        assert!(write_err("MATCH (n:Company {ticker: 'A'}) REMOVE m:Listed")
            .contains("anchor variable"));
        // A DELETE naming a non-anchor variable is rejected too.
        assert!(
            write_err("MATCH (n:Company {ticker: 'A'}) DELETE n, m").contains("DELETE must target")
        );
    }

    #[test]
    fn plain_write_forbids_computed_map_values() {
        // A non-UNWIND replace/merge map may not carry a computed right-hand side.
        assert!(
            write_err("MATCH (n:Company {ticker: 'A'}) SET n += {x: n.y + 1}").contains("literal")
        );
    }

    // ── Stage 7: CREATE, MERGE ON CREATE / ON MATCH SET ─────────────────────────

    #[test]
    fn create_lowers_with_all_inline_props() {
        let c = match parse_statement("CREATE (n:Person {name: 'Zoe', age: 1})").unwrap() {
            Statement::Create(c) => c,
            other => panic!("expected a Create, got {other:?}"),
        };
        assert_eq!(c.var, "n");
        assert_eq!(c.label, "Person");
        assert_eq!(
            c.props,
            vec![
                ("name".to_string(), Expr::Literal(Value::Str("Zoe".into()))),
                ("age".to_string(), Expr::Literal(Value::Int(1))),
            ]
        );
    }

    #[test]
    fn create_requires_a_name_a_label_and_a_prop() {
        assert!(write_err("CREATE (:Person {name: 'Zoe'})").contains("named"));
        assert!(write_err("CREATE (n {name: 'Zoe'})").contains("one label"));
        assert!(write_err("CREATE (n:Person)").contains("at least one inline property"));
    }

    #[test]
    fn merge_on_create_and_on_match_lower() {
        let w = write(
            "MERGE (n:Person {name: 'Zoe'}) ON CREATE SET n.created = true ON MATCH SET n.seen = true",
        );
        assert!(w.upsert, "ON CREATE/ON MATCH imply a MERGE");
        assert_eq!(
            w.on_create,
            vec![SetItem::Prop {
                prop: "created".to_string(),
                value: Expr::Literal(Value::Bool(true)),
            }]
        );
        assert_eq!(
            w.on_match,
            vec![SetItem::Prop {
                prop: "seen".to_string(),
                value: Expr::Literal(Value::Bool(true)),
            }]
        );
        assert_eq!(w.op, WriteOp::Set(vec![]), "no unconditional mutation");
    }

    #[test]
    fn merge_conditional_and_unconditional_set_coexist() {
        let w = write("MERGE (n:Person {name: 'Zoe'}) ON CREATE SET n.a = 1 SET n.b = 2");
        assert_eq!(
            w.on_create,
            vec![SetItem::Prop {
                prop: "a".to_string(),
                value: Expr::Literal(Value::Int(1)),
            }]
        );
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::Prop {
                prop: "b".to_string(),
                value: Expr::Literal(Value::Int(2)),
            }])
        );
    }

    #[test]
    fn on_create_on_match_rejected_on_match_anchor() {
        assert!(
            write_err("MATCH (n:Person {name: 'Zoe'}) ON CREATE SET n.x = 1")
                .contains("only valid on MERGE")
        );
        assert!(
            write_err("MATCH (n:Person {name: 'Zoe'}) ON MATCH SET n.x = 1")
                .contains("only valid on MERGE")
        );
    }

    #[test]
    fn merge_lowers_to_an_upsert_anchor() {
        // MERGE carries the same shape as MATCH … SET but flags create-if-absent.
        let w = write("MERGE (n:Company {ticker: 'ABC'}) SET n.price = 10");
        assert!(w.upsert, "MERGE sets the upsert flag");
        assert_eq!(w.label, "Company");
        assert_eq!(w.key, "ticker");
        assert_eq!(w.key_value, Expr::Literal(Value::Str("ABC".into())));
        assert_eq!(
            w.op,
            WriteOp::Set(vec![SetItem::Prop {
                prop: "price".to_string(),
                value: Expr::Literal(Value::Int(10)),
            }])
        );

        // MATCH is update-only (no create).
        let m = write("MATCH (n:Company {ticker: 'ABC'}) SET n.price = 10");
        assert!(!m.upsert, "MATCH does not set the upsert flag");
    }

    #[test]
    fn merge_cannot_delete() {
        // MERGE … DELETE is nonsensical (create-or-match then remove) — rejected.
        let e = write_err("MERGE (n:Company {ticker: 'ABC'}) DELETE n");
        assert!(
            e.contains("MERGE cannot be combined with DELETE") || e == "read-only",
            "got: {e}"
        );
    }

    #[test]
    fn merge_edge_lowers_to_a_create() {
        let w = edge_write("MERGE (a:Person {name: 'Ann'})-[:KNOWS]->(b:Person {name: 'Bob'})");
        assert_eq!(w.op, EdgeWriteOp::Create);
        assert_eq!(w.reltype, "KNOWS");
        assert_eq!(w.src.label, "Person");
        assert_eq!(w.src.key, "name");
        assert_eq!(w.src.key_value, Expr::Literal(Value::Str("Ann".into())));
        assert_eq!(w.dst.label, "Person");
        assert_eq!(w.dst.key_value, Expr::Literal(Value::Str("Bob".into())));
    }

    #[test]
    fn merge_edge_accepts_params_and_an_ignored_rel_var() {
        let w = edge_write("MERGE (a:Company {ticker: $s})-[r:OWNS]->(b:Drug {id: $d})");
        assert_eq!(w.reltype, "OWNS");
        assert_eq!(w.src.key_value, Expr::Param("s".into()));
        assert_eq!(w.dst.key_value, Expr::Param("d".into()));
    }

    #[test]
    fn merge_edge_lowers_set_properties() {
        let w = edge_write(
            "MERGE (a:Person {name: 'Ann'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020, r.weight = $w",
        );
        assert_eq!(w.op, EdgeWriteOp::Create);
        assert_eq!(
            w.sets,
            vec![
                ("since".to_string(), Expr::Literal(Value::Int(2020))),
                ("weight".to_string(), Expr::Param("w".into())),
            ]
        );
        // A bare MERGE (and a DELETE) carries no SET.
        assert!(
            edge_write("MERGE (a:Person {name: 'Ann'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                .sets
                .is_empty()
        );
    }

    #[test]
    fn edge_set_requires_a_named_rel_var_and_constant_values() {
        // SET without naming the relationship is rejected.
        let e = write_err(
            "MERGE (a:Person {name: 'Ann'})-[:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020",
        );
        assert!(
            e.contains("name the relationship") || e == "read-only",
            "got: {e}"
        );
        // SET targeting a variable other than the relationship is rejected.
        let e = write_err(
            "MERGE (a:Person {name: 'Ann'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET x.since = 2020",
        );
        assert!(
            e.contains("SET must target the relationship variable") || e == "read-only",
            "got: {e}"
        );
        // A non-constant SET value is rejected.
        let e = write_err(
            "MERGE (a:Person {name: 'Ann'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = a.x",
        );
        assert!(
            e.contains("must be a literal or a parameter") || e == "read-only",
            "got: {e}"
        );
    }

    #[test]
    fn delete_edge_lowers_and_checks_the_rel_var() {
        let w = edge_write(
            "MATCH (a:Person {name: 'Ann'})-[r:KNOWS]->(b:Person {name: 'Bob'}) DELETE r",
        );
        assert_eq!(w.op, EdgeWriteOp::Delete);
        assert_eq!(w.reltype, "KNOWS");

        // DELETE must name the bound relationship variable.
        let e = write_err(
            "MATCH (a:Person {name: 'Ann'})-[r:KNOWS]->(b:Person {name: 'Bob'}) DELETE x",
        );
        assert!(
            e.contains("DELETE must target the relationship variable") || e == "read-only",
            "got: {e}"
        );

        // A rel variable is required to delete (there is nothing to name otherwise).
        let e =
            write_err("MATCH (a:Person {name: 'Ann'})-[:KNOWS]->(b:Person {name: 'Bob'}) DELETE r");
        assert!(e.contains("name it") || e == "read-only", "got: {e}");
    }

    #[test]
    fn edge_write_rejects_unsupported_shapes() {
        // Undirected / right-to-left relationships are not writes.
        assert!(edge_or_read_reject(
            "MERGE (a:Person {name: 'Ann'})-[:KNOWS]-(b:Person {name: 'Bob'})"
        ));
        assert!(edge_or_read_reject(
            "MERGE (a:Person {name: 'Ann'})<-[:KNOWS]-(b:Person {name: 'Bob'})"
        ));
        // Variable-length is not a write.
        assert!(edge_or_read_reject(
            "MERGE (a:Person {name: 'Ann'})-[:KNOWS*1..2]->(b:Person {name: 'Bob'})"
        ));
        // An endpoint needs exactly one label + one business-key property.
        assert!(edge_or_read_reject(
            "MERGE (a:Person)-[:KNOWS]->(b:Person {name: 'Bob'})"
        ));
    }

    /// A write shape that must not lower to an `EdgeWriteStmt` — either it errors, or
    /// it falls through to the read parser (which also rejects it).
    fn edge_or_read_reject(q: &str) -> bool {
        !matches!(parse_statement(q), Ok(Statement::WriteEdge(_)))
    }

    #[test]
    fn read_query_is_not_a_write() {
        assert!(matches!(
            parse_statement("MATCH (n:Person) RETURN n").unwrap(),
            Statement::Read(_)
        ));
    }

    #[test]
    fn unsupported_write_shapes_are_rejected() {
        // No label on the anchor.
        assert!(write_err("MATCH (n {ticker: 'A'}) SET n.p = 1").contains("label"));
        // No inline business key.
        assert!(write_err("MATCH (n:Company) SET n.p = 1").contains("business-key"));
        // SET targets a different variable than the anchor.
        assert!(
            write_err("MATCH (n:Company {ticker: 'A'}) SET m.p = 1").contains("anchor variable")
        );
        // Computed right-hand side is out of scope for Phase 1c.
        assert!(write_err("MATCH (n:Company {ticker: 'A'}) SET n.p = n.p + 1").contains("literal"));
        // Anonymous anchor node.
        assert!(write_err("MATCH (:Company {ticker: 'A'}) SET n.p = 1").contains("named"));
        // Multi-node writes and relationships fall through to read-only rejection.
        assert!(
            write_err("MATCH (a:Company {ticker:'A'})-[:OWNS]->(b) SET a.p = 1")
                .contains("read-only")
        );
    }

    #[test]
    fn disabled_layer_still_rejects_writes_via_parse() {
        // `parse` (the read entry the server uses when the writable layer is off)
        // must keep rejecting a SET as read-only.
        assert!(err("MATCH (n:Company {ticker:'A'}) SET n.p = 1").contains("read-only"));
    }

    /// The WHERE expression of the first MATCH clause (for predicate-lowering
    /// tests).
    fn match_where(q: &Query) -> &Expr {
        let Clause::Match(m) = &q.head.reading[0] else {
            panic!("expected a MATCH clause");
        };
        m.where_.as_ref().expect("MATCH has no WHERE")
    }

    /// The accept corpus — representative of the widened read subset (and the
    /// sibling services' real query strings).
    #[test]
    fn accepts_the_read_subset() {
        let corpus = [
            "MATCH (n) RETURN n",
            "MATCH (n:Person) RETURN n.name AS name",
            "MATCH (n:Person {name: 'Alice'}) RETURN n",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name",
            "MATCH (a)-[r:KNOWS|FOLLOWS*1..3]->(b) RETURN b",
            "MATCH (a)<-[:KNOWS]-(b) RETURN b",
            "MATCH (a)-[:KNOWS*]-(b) RETURN b",
            "MATCH (n:Person) WHERE n.age > 30 AND n.name STARTS WITH 'A' RETURN n",
            "MATCH (n) WHERE n:Person RETURN count(n)",
            "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC SKIP 1 LIMIT 10",
            "MATCH (n:Person) RETURN DISTINCT n.city",
            "MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c > 1 RETURN city, c",
            "MATCH (n:Person) RETURN n {.name, .age}",
            "RETURN 1 + 2 * 3 AS v",
            "MATCH (n) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END AS band",
            "MATCH (n) WHERE n.age IN [30, 40, 50] RETURN n",
            "MATCH (n) WHERE n.name IS NOT NULL RETURN n",
            "MATCH (n) WHERE any(x IN n.tags WHERE x = 'vip') RETURN n",
            "MATCH (n:Person) RETURN n.name UNION MATCH (n:Company) RETURN n.name",
            "MATCH (n:Person) RETURN n.name UNION ALL MATCH (c:Company) RETURN c.name AS name",
            "MATCH (n) RETURN n.score * -1 AS neg",
            "MATCH (n) RETURN n.embedding[0] AS first",
            "MATCH (n) RETURN collect(DISTINCT n.city) AS cities",
            "RETURN $limit AS l",
            // §1 list comprehension (filter / map / both) and §1+index.
            "RETURN [x IN [1, 2, 3] WHERE x > 1] AS r",
            "RETURN [x IN [1, 2, 3] | x * 2] AS r",
            "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS r",
            "MATCH (n) RETURN [l IN labels(n) WHERE l <> 'Concept'][0] AS primary",
            // §2 pattern comprehension.
            "MATCH (n) RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
            "MATCH (n) RETURN size([(n)<-[:SOURCED_FROM]-(:Chunk) | 1]) AS deg",
            // §3 UNWIND (now a read clause, not forbidden).
            "UNWIND [1, 2, 3] AS x RETURN x",
            "MATCH (a)-[r*1..2]->(b) WITH r LIMIT 1 UNWIND r AS e RETURN type(e)",
            // §4 startNode / endNode.
            "MATCH ()-[r]->() RETURN startNode(r).name AS s, endNode(r).name AS e",
        ];
        for q in corpus {
            ok(q);
        }
    }

    #[test]
    fn comprehension_and_unwind_lower_to_expected_ast() {
        // List comprehension with both filter and projection.
        let q = ok("RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS r");
        match &q.head.ret.body.items[0].expr {
            Expr::ListComprehension {
                var,
                predicate,
                projection,
                ..
            } => {
                assert_eq!(var, "x");
                assert!(predicate.is_some());
                assert!(projection.is_some());
            }
            other => panic!("expected ListComprehension, got {other:?}"),
        }

        // Pattern comprehension: pattern + mandatory projection, no filter.
        let q = ok("MATCH (n) RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends");
        assert!(matches!(
            &q.head.ret.body.items[0].expr,
            Expr::PatternComprehension {
                predicate: None,
                ..
            }
        ));

        // `[a IN b]` (no WHERE/`|`) stays a one-element list literal (membership).
        let q = ok("RETURN [2 IN [1, 2, 3]] AS r");
        match &q.head.ret.body.items[0].expr {
            Expr::List(items) => {
                assert_eq!(items.len(), 1);
                assert!(matches!(items[0], Expr::In(_, _)));
            }
            other => panic!("expected a list literal, got {other:?}"),
        }

        // UNWIND lowers to a reading clause carrying expr + alias.
        let q = ok("UNWIND [1, 2, 3] AS x RETURN x");
        match &q.head.reading[0] {
            Clause::Unwind(uc) => {
                assert_eq!(uc.var, "x");
                assert!(matches!(uc.expr, Expr::List(_)));
            }
            other => panic!("expected an Unwind clause, got {other:?}"),
        }
    }

    #[test]
    fn rejects_writes_and_procedures_with_read_only_message() {
        for (q, kw) in [
            ("CREATE (n) RETURN n", "CREATE"),
            ("MATCH (n) SET n.x = 1 RETURN n", "SET"),
            ("MATCH (n) DELETE n", "DELETE"),
            ("MATCH (n) DETACH DELETE n", "DETACH"),
            ("MERGE (n:Person {name: 'A'}) RETURN n", "MERGE"),
            ("MATCH (n) REMOVE n:Person RETURN n", "REMOVE"),
            // Every procedure call is rejected EXCEPT the one vector KNN form.
            ("CALL db.labels() YIELD label RETURN label", "CALL"),
            (
                "CALL db.idx.fulltext.queryNodes('L', 'q') YIELD node RETURN node",
                "CALL",
            ),
        ] {
            let e = err(q);
            assert!(
                e.contains("read-only") && e.contains(kw),
                "for {q:?} expected read-only/{kw}, got: {e}"
            );
        }
    }

    #[test]
    fn accepts_the_vector_knn_procedure() {
        // Bare YIELD node, score (the FalkorDB shape) and an aliased / RETURN form.
        let q = ok(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 5, vecf32([0.1, 0.2, 0.3])) YIELD node, score RETURN node, score",
        );
        let Clause::VectorCall(vc) = &q.head.reading[0] else {
            panic!("expected a VectorCall clause");
        };
        assert_eq!(vc.label, "Person");
        assert_eq!(vc.property, "embedding");
        assert!(matches!(vc.k, Expr::Literal(Value::Int(5))));
        assert!(
            matches!(&vc.query_vec, Expr::Function { name, .. } if name == "vecf32"),
            "query vector should lower to a vecf32(...) call"
        );
        assert_eq!(
            vc.yields,
            vec![
                ("node".to_string(), "node".to_string()),
                ("score".to_string(), "score".to_string())
            ]
        );

        // Parameterised query vector + k, aliased yields, trailing WHERE/ORDER BY.
        let q = ok(
            "CALL db.idx.vector.queryNodes('Doc', 'vec', $k, $q) YIELD node AS n, score AS s WHERE s < 0.5 RETURN n.title, s ORDER BY s",
        );
        let Clause::VectorCall(vc) = &q.head.reading[0] else {
            panic!("expected a VectorCall clause");
        };
        assert!(matches!(vc.k, Expr::Param(ref p) if p == "k"));
        assert!(matches!(vc.query_vec, Expr::Param(ref p) if p == "q"));
        assert_eq!(
            vc.yields,
            vec![
                ("node".to_string(), "n".to_string()),
                ("score".to_string(), "s".to_string())
            ]
        );
    }

    #[test]
    fn rejects_malformed_vector_calls() {
        // Wrong arity, non-string label, unknown yield.
        assert!(
            parse("CALL db.idx.vector.queryNodes('L', 'p', 5) YIELD node RETURN node").is_err()
        );
        assert!(parse(
            "CALL db.idx.vector.queryNodes(1, 'p', 5, vecf32([0.1])) YIELD node RETURN node"
        )
        .is_err());
        assert!(parse(
            "CALL db.idx.vector.queryNodes('L', 'p', 5, vecf32([0.1])) YIELD bogus RETURN bogus"
        )
        .is_err());
    }

    #[test]
    fn lowers_metadata_call_clause() {
        // Bare standalone call (no YIELD, no RETURN): a synthetic RETURN * is added.
        let q = ok("CALL db.meta.stats()");
        let Clause::Call(cc) = &q.head.reading[0] else {
            panic!("expected a Call clause, got {:?}", q.head.reading[0]);
        };
        assert_eq!(cc.name.to_ascii_lowercase(), "db.meta.stats");
        assert!(cc.yields.is_empty());
        assert!(cc.where_.is_none());
        assert!(q.head.ret.body.star, "bare call synthesises RETURN *");

        // YIELD + WHERE + RETURN form (FalkorDB test11/test12 shape).
        let q = ok(
            "CALL dbms.functions() YIELD name AS fn, aggregation WHERE aggregation = true \
             RETURN fn ORDER BY fn",
        );
        let Clause::Call(cc) = &q.head.reading[0] else {
            panic!("expected a Call clause");
        };
        assert_eq!(cc.name.to_ascii_lowercase(), "dbms.functions");
        assert_eq!(
            cc.yields,
            vec![
                ("name".to_string(), "fn".to_string()),
                ("aggregation".to_string(), "aggregation".to_string()),
            ]
        );
        assert!(cc.where_.is_some());
        assert!(!q.head.ret.body.star);
    }

    #[test]
    fn parse_statement_recognises_the_consolidate_trigger() {
        // The exact shape — case-insensitive, whitespace-tolerant, optional `;` — is
        // the Phase 5 consolidation trigger.
        for q in [
            "CALL slater.consolidate()",
            "call slater.consolidate()",
            "  CALL   slater.consolidate ()  ",
            "CALL slater.consolidate();",
        ] {
            assert!(
                matches!(parse_statement(q), Ok(Statement::Consolidate)),
                "expected Consolidate for {q:?}"
            );
        }
        // Arguments, a trailing YIELD/RETURN, or a longer name are not the trigger:
        // they fall through to the read parser (which rejects the CALL as read-only).
        for q in [
            "CALL slater.consolidate(1)",
            "CALL slater.consolidate() YIELD x RETURN x",
            "CALL slater.consolidateX()",
        ] {
            assert!(
                !matches!(parse_statement(q), Ok(Statement::Consolidate)),
                "should not be the consolidate trigger: {q:?}"
            );
        }
        // With the writable layer off the server calls `parse`, which — the proc being
        // deliberately absent from the read-only whitelist — rejects it as a forbidden
        // (write/admin) CALL rather than serving it as a metadata read.
        assert!(parse("CALL slater.consolidate()").is_err());
    }

    #[test]
    fn metadata_call_whitelist_only() {
        // The read-only metadata + algo procs parse; every other CALL stays rejected
        // as read-only (the whitelist is exactly vector + metadata + algo.*).
        for q in [
            "CALL db.meta.stats()",
            "CALL db.constraints()",
            "CALL dbms.procedures()",
            "CALL dbms.functions()",
        ] {
            assert!(parse(q).is_ok(), "expected {q:?} to parse");
        }
        for q in [
            "CALL db.labels()",
            "CALL dbms.security.listUsers()",
            "CALL algo.stronglyConnectedComponents()", // not a whitelisted algo name
        ] {
            let e = err(q);
            assert!(
                e.contains("read-only"),
                "for {q:?} expected read-only, got: {e}"
            );
        }
    }

    #[test]
    fn lowers_algo_call_clause() {
        // algo.* procs parse through `call_clause`, capturing arguments (which may
        // reference bound variables) and the YIELD list.
        let q = ok(
            "MATCH (a:Person) CALL algo.BFS(a, -1, 'KNOWS') YIELD nodes, edges \
             RETURN nodes, edges",
        );
        let Clause::Call(cc) = &q.head.reading[1] else {
            panic!("expected a Call clause, got {:?}", q.head.reading);
        };
        assert_eq!(cc.name.to_ascii_lowercase(), "algo.bfs");
        assert_eq!(cc.args.len(), 3);
        assert_eq!(
            cc.yields,
            vec![
                ("nodes".to_string(), "nodes".to_string()),
                ("edges".to_string(), "edges".to_string()),
            ]
        );

        // Config-map argument + standalone (synthetic RETURN *) form.
        let q = ok("CALL algo.WCC({relationshipTypes: ['KNOWS']})");
        let Clause::Call(cc) = &q.head.reading[0] else {
            panic!("expected a Call clause");
        };
        assert_eq!(cc.name.to_ascii_lowercase(), "algo.wcc");
        assert_eq!(cc.args.len(), 1);
        assert!(q.head.ret.body.star, "bare call synthesises RETURN *");

        // Zero-arg form parses too.
        assert!(parse("CALL algo.betweenness()").is_ok());
    }

    #[test]
    fn rejects_syntax_errors() {
        for q in [
            "MATCH RETURN n",           // no pattern
            "MATCH (n) RETURN",         // no projection
            "MATCH (n) WHERE RETURN n", // empty predicate
            "RETURN",                   // nothing to return
            "MATCH (n) RETURN n ORDER", // dangling ORDER
            "MATCH (n RETURN n",        // unbalanced paren
        ] {
            assert!(parse(q).is_err(), "expected syntax error for {q:?}");
        }
    }

    #[test]
    fn lowers_pattern_and_projection_structurally() {
        let q = ok("MATCH (a:Person)-[r:KNOWS*1..3]->(b) WHERE a.age > 30 RETURN a.name AS name ORDER BY a.age DESC LIMIT 5");
        assert!(q.tail.is_empty());
        assert_eq!(q.head.reading.len(), 1);
        let Clause::Match(m) = &q.head.reading[0] else {
            panic!("expected MATCH");
        };
        assert!(!m.optional);
        assert!(m.where_.is_some());
        let p = &m.patterns[0];
        assert_eq!(p.start.var.as_deref(), Some("a"));
        assert_eq!(
            p.start.label_expr,
            Some(LabelExpr::Atom("Person".to_string()))
        );
        assert_eq!(p.rels.len(), 1);
        let (rel, end) = &p.rels[0];
        assert_eq!(rel.dir, Direction::Outgoing);
        assert_eq!(rel.type_expr, Some(LabelExpr::Atom("KNOWS".to_string())));
        assert_eq!(
            rel.var_length,
            Some(VarLength {
                min: Some(1),
                max: Some(3)
            })
        );
        assert_eq!(end.var.as_deref(), Some("b"));
        // RETURN body.
        assert!(!q.head.ret.distinct);
        assert_eq!(q.head.ret.body.items.len(), 1);
        assert_eq!(q.head.ret.body.items[0].alias.as_deref(), Some("name"));
        assert_eq!(q.head.ret.body.order_by.len(), 1);
        assert_eq!(q.head.ret.body.order_by[0].1, SortDir::Desc);
    }

    #[test]
    fn lowers_union_and_distinct() {
        let q = ok("MATCH (n:Person) RETURN DISTINCT n.name UNION ALL MATCH (c:Company) RETURN c.name AS name");
        assert!(q.head.ret.distinct);
        assert_eq!(q.tail.len(), 1);
        assert!(q.tail[0].0, "UNION ALL should set the all flag");
    }

    #[test]
    fn lowers_expression_precedence() {
        // 1 + 2 * 3 parses as 1 + (2 * 3).
        let q = ok("RETURN 1 + 2 * 3 AS v");
        let e = &q.head.ret.body.items[0].expr;
        match e {
            Expr::Arith(BinOp::Add, l, r) => {
                assert_eq!(**l, Expr::Literal(Value::Int(1)));
                assert!(matches!(**r, Expr::Arith(BinOp::Mul, _, _)));
            }
            other => panic!("expected Add at top, got {other:?}"),
        }
    }

    #[test]
    fn power_operator_precedence_and_associativity() {
        // `^` binds tighter than `*` and looser than a unary sign, and is
        // left-associative — matching the openCypher reference grammar.
        // 2 * 3 ^ 2 == 2 * (3 ^ 2).
        let q = ok("RETURN 2 * 3 ^ 2 AS v");
        match &q.head.ret.body.items[0].expr {
            Expr::Arith(BinOp::Mul, l, r) => {
                assert_eq!(**l, Expr::Literal(Value::Int(2)));
                assert!(matches!(**r, Expr::Arith(BinOp::Pow, _, _)));
            }
            other => panic!("expected Mul at top, got {other:?}"),
        }
        // -2 ^ 2 == (-2) ^ 2: the unary sign is part of the left operand.
        let q = ok("RETURN -2 ^ 2 AS v");
        assert!(matches!(
            &q.head.ret.body.items[0].expr,
            Expr::Arith(BinOp::Pow, l, _) if matches!(**l, Expr::Neg(_))
        ));
        // 2 ^ 3 ^ 2 == (2 ^ 3) ^ 2 (left-assoc).
        let q = ok("RETURN 2 ^ 3 ^ 2 AS v");
        assert!(matches!(
            &q.head.ret.body.items[0].expr,
            Expr::Arith(BinOp::Pow, l, _) if matches!(**l, Expr::Arith(BinOp::Pow, _, _))
        ));
    }

    #[test]
    fn accepts_opencypher_lexical_extensions() {
        // Gaps closed against the openCypher reference grammar: trailing `;`,
        // block comments, exponent/leading-dot floats, and the `^` operator.
        for q in [
            "MATCH (n) RETURN n;",            // trailing semicolon
            "MATCH (n) RETURN n ;",           // …with space before it
            "MATCH (n) /* block */ RETURN n", // inline block comment
            "/* leading */ RETURN 1 AS x",    // leading block comment
            "RETURN 1 /* a */ + /* b */ 2 AS x",
            "RETURN 1e10 AS x",   // exponent, no decimal point
            "RETURN 1E10 AS x",   // capital E
            "RETURN 1.5e-3 AS x", // signed exponent
            "RETURN .5 AS x",     // leading-dot float
            "RETURN -.5 AS x",
            "RETURN 2 ^ 3 AS x", // power operator
        ] {
            assert!(parse(q).is_ok(), "expected {q:?} to parse");
        }
        // A plain integer still lowers to Int (float must not shadow it).
        let q = ok("RETURN 123 AS x");
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::Literal(Value::Int(123))
        );
        // The new float forms lower to Float with the right value.
        for (src, want) in [("1e3", 1000.0_f64), (".5", 0.5), ("1.5e-3", 0.0015)] {
            let q = ok(&format!("RETURN {src} AS x"));
            assert_eq!(
                q.head.ret.body.items[0].expr,
                Expr::Literal(Value::Float(want)),
                "for {src:?}"
            );
        }
    }

    #[test]
    fn lowers_namespaced_function_name() {
        // The `func_name` grammar rule accepts a dotted namespace; the whole
        // path is preserved as the function name (`list.sort`, not `list`).
        let q = ok("RETURN list.sort([3,1,2], false) AS s");
        match &q.head.ret.body.items[0].expr {
            Expr::Function { name, args, .. } => {
                assert_eq!(name, "list.sort");
                assert!(matches!(args, FuncArgs::Args(a) if a.len() == 2));
            }
            other => panic!("expected a Function, got {other:?}"),
        }
        // A dotted path without a call is still a property access, not a function.
        let q = ok("MATCH (n) RETURN n.name AS x");
        assert!(matches!(
            &q.head.ret.body.items[0].expr,
            Expr::Property(_, k) if k == "name"
        ));
    }

    #[test]
    fn string_literals_unescape() {
        let q = ok(r#"RETURN 'a\'b\nc' AS s"#);
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::Literal(Value::Str("a'b\nc".to_string()))
        );
    }

    #[test]
    fn unknown_escape_keeps_backslash() {
        // `\w` is not a recognised escape; the backslash survives so regex
        // patterns reach the engine intact (FalkorDB / libcypher-parser parity).
        let q = ok(r"RETURN '\w\d' AS s");
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::Literal(Value::Str(r"\w\d".to_string()))
        );
    }

    #[test]
    fn lowers_regex_match_operator() {
        let q = ok("RETURN 'abc' =~ 'a.*'");
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::StringOp(
                StrOp::Regex,
                Box::new(Expr::Literal(Value::Str("abc".to_string()))),
                Box::new(Expr::Literal(Value::Str("a.*".to_string()))),
            )
        );
    }

    #[test]
    fn lowers_slice_with_open_ends() {
        // Both bounds present.
        let q = ok("WITH [1,2,3] AS l RETURN l[1..2]");
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::Slice {
                base: Box::new(Expr::Var("l".to_string())),
                from: Some(Box::new(Expr::Literal(Value::Int(1)))),
                to: Some(Box::new(Expr::Literal(Value::Int(2)))),
            }
        );

        // Open start / open end / fully open lower to `None` bounds.
        let open_start = ok("WITH [1] AS l RETURN l[..2]");
        assert!(matches!(
            open_start.head.ret.body.items[0].expr,
            Expr::Slice {
                from: None,
                to: Some(_),
                ..
            }
        ));
        let open_end = ok("WITH [1] AS l RETURN l[1..]");
        assert!(matches!(
            open_end.head.ret.body.items[0].expr,
            Expr::Slice {
                from: Some(_),
                to: None,
                ..
            }
        ));
        let both = ok("WITH [1] AS l RETURN l[..]");
        assert!(matches!(
            both.head.ret.body.items[0].expr,
            Expr::Slice {
                from: None,
                to: None,
                ..
            }
        ));
    }

    #[test]
    fn plain_subscript_still_lowers_to_index() {
        // A non-slice subscript must backtrack to `index_access`.
        let q = ok("WITH [1,2,3] AS l RETURN l[0]");
        assert!(matches!(q.head.ret.body.items[0].expr, Expr::Index(..)));
    }

    #[test]
    fn lowers_reduce() {
        let q = ok("RETURN reduce(s = 0, n IN [1,2,3] | s + n)");
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::Reduce {
                acc_var: "s".to_string(),
                acc_init: Box::new(Expr::Literal(Value::Int(0))),
                var: "n".to_string(),
                list: Box::new(Expr::List(vec![
                    Expr::Literal(Value::Int(1)),
                    Expr::Literal(Value::Int(2)),
                    Expr::Literal(Value::Int(3)),
                ])),
                body: Box::new(Expr::Arith(
                    BinOp::Add,
                    Box::new(Expr::Var("s".to_string())),
                    Box::new(Expr::Var("n".to_string())),
                )),
            }
        );
    }

    #[test]
    fn reduce_missing_sections_rejected() {
        // A reduce missing its `| body` parses as a plain function call (matching
        // FalkorDB, which then rejects it as "Unknown function 'reduce'" at
        // resolution rather than as a syntax error).
        let q = ok("RETURN reduce(s = 0, n IN [1,2,3])");
        assert!(matches!(
            q.head.ret.body.items[0].expr,
            Expr::Function { ref name, .. } if name == "reduce"
        ));
        // Missing accumulator init (a bare `|` with no preceding args) is a
        // genuine syntax error.
        assert!(!err("RETURN reduce(n IN [1,2,3] | n)").is_empty());
    }

    #[test]
    fn lowers_pattern_predicate() {
        // A bare relationship pattern in WHERE lowers to a PatternPredicate whose
        // pattern has no path var, the anchor node, and one outgoing rel.
        let q = ok("MATCH (n) WHERE (n)-[:KNOWS]->() RETURN n");
        match match_where(&q) {
            Expr::PatternPredicate(p) => {
                assert!(p.path_var.is_none());
                assert_eq!(p.start.var.as_deref(), Some("n"));
                assert_eq!(p.rels.len(), 1);
                assert_eq!(p.rels[0].0.dir, Direction::Outgoing);
                assert_eq!(
                    p.rels[0].0.type_expr,
                    Some(LabelExpr::Atom("KNOWS".to_string()))
                );
            }
            other => panic!("expected PatternPredicate, got {other:?}"),
        }

        // The negated form is a Not wrapping the predicate (anti-semi-apply).
        let q = ok("MATCH (n) WHERE NOT (n)-->() RETURN n");
        assert!(matches!(
            match_where(&q),
            Expr::Not(inner) if matches!(**inner, Expr::PatternPredicate(_))
        ));
    }

    #[test]
    fn bare_parens_is_not_a_pattern_predicate() {
        // `(n)` with no relationship must backtrack to `parens`/`variable`, not a
        // pattern predicate (which requires ≥1 relationship).
        let q = ok("MATCH (n) WHERE (n) RETURN n");
        assert!(matches!(match_where(&q), Expr::Var(v) if v == "n"));
        // A parenthesised arithmetic expression is likewise untouched.
        let q = ok("RETURN (1 + 2) AS v");
        assert!(matches!(q.head.ret.body.items[0].expr, Expr::Arith(..)));
    }

    #[test]
    fn lowers_exists_subquery() {
        // Pattern-only inner form (no MATCH keyword), no WHERE.
        let q = ok("MATCH (n) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n");
        match match_where(&q) {
            Expr::Exists {
                patterns,
                predicate,
            } => {
                assert_eq!(patterns.len(), 1);
                assert!(predicate.is_none());
            }
            other => panic!("expected Exists, got {other:?}"),
        }

        // Explicit MATCH keyword + inner WHERE.
        let q = ok("MATCH (n) WHERE EXISTS { MATCH (n)-->(m) WHERE n.age > m.age } RETURN n");
        match match_where(&q) {
            Expr::Exists {
                patterns,
                predicate,
            } => {
                assert_eq!(patterns.len(), 1);
                assert!(predicate.is_some());
            }
            other => panic!("expected Exists, got {other:?}"),
        }

        // `exists(x)` (parenthesised) remains the scalar property-existence
        // function — only `exists { … }` is the subquery.
        let q = ok("MATCH (n) WHERE exists(n.name) RETURN n");
        assert!(matches!(
            match_where(&q),
            Expr::Function { name, .. } if name == "exists"
        ));
    }

    #[test]
    fn lowers_shortest_path() {
        // shortestPath wraps a pattern (parsed as a pattern, not function args).
        let q = ok("MATCH (a), (b) WHERE shortestPath((a)-[:KNOWS*]->(b)) RETURN a");
        match match_where(&q) {
            Expr::ShortestPath(pat) => {
                assert_eq!(pat.start.var.as_deref(), Some("a"));
                assert_eq!(pat.rels.len(), 1);
                assert_eq!(pat.rels[0].1.var.as_deref(), Some("b"));
                assert_eq!(pat.rels[0].0.dir, Direction::Outgoing);
            }
            other => panic!("expected ShortestPath, got {other:?}"),
        }
    }

    #[test]
    fn binds_path_variable() {
        // `p = …` records the path variable on the MATCH pattern.
        let q = ok("MATCH p = (a)-[:KNOWS]->(b) RETURN p");
        let Clause::Match(m) = &q.head.reading[0] else {
            panic!("expected a MATCH clause");
        };
        assert_eq!(m.patterns[0].path_var.as_deref(), Some("p"));
    }

    // ── GQL quantified path patterns ─────────────────────────────────────────

    /// Pull the first pattern out of a single-MATCH query.
    fn first_pattern(q: &Query) -> &Pattern {
        let Clause::Match(m) = &q.head.reading[0] else {
            panic!("expected a MATCH clause");
        };
        &m.patterns[0]
    }

    #[test]
    fn ordinary_pattern_has_no_segments() {
        // A quantifier-free pattern lowers exactly as before: the whole chain lives
        // in `rels` and `segments` stays `None`, so the hot path is untouched.
        let p = &ok("MATCH (a:Person)-[:KNOWS]->(b) RETURN b").head;
        let Clause::Match(m) = &p.reading[0] else {
            panic!("expected MATCH");
        };
        assert!(m.patterns[0].segments.is_none());
        assert_eq!(m.patterns[0].rels.len(), 1);
    }

    #[test]
    fn lowers_quantified_range() {
        let q = ok("MATCH (a:Person) ((x)-[:KNOWS]->(y)){1,3} (b) RETURN b");
        let p = first_pattern(&q);
        assert_eq!(p.start.var.as_deref(), Some("a"));
        assert!(p.rels.is_empty(), "quantified pattern keeps rels empty");
        let segs = p.segments.as_ref().expect("segments populated");
        assert_eq!(segs.len(), 1);
        match &segs[0] {
            Segment::Quantified {
                inner,
                bounds,
                exit,
            } => {
                assert_eq!(inner.len(), 1);
                assert_eq!(
                    inner[0].0.type_expr,
                    Some(LabelExpr::Atom("KNOWS".to_string()))
                );
                assert_eq!(inner[0].0.dir, Direction::Outgoing);
                assert_eq!(bounds.min, Some(1));
                assert_eq!(bounds.max, Some(3));
                assert_eq!(exit.var.as_deref(), Some("b"));
            }
            other => panic!("expected Quantified, got {other:?}"),
        }
    }

    #[test]
    fn lowers_quantifier_bound_forms() {
        let cases = [
            ("{2}", Some(2u32), Some(2u32)),
            ("{2,5}", Some(2), Some(5)),
            ("{2,}", Some(2), None),
            ("{,5}", None, Some(5)),
            ("+", Some(1), None),
            ("*", Some(0), None),
        ];
        for (q, min, max) in cases {
            let src = format!("MATCH (a) ((x)-[:R]->(y)){q} (b) RETURN b");
            let parsed = ok(&src);
            let p = first_pattern(&parsed);
            let segs = p.segments.as_ref().unwrap();
            match &segs[0] {
                Segment::Quantified { bounds, .. } => {
                    assert_eq!(bounds.min, min, "min for {q}");
                    assert_eq!(bounds.max, max, "max for {q}");
                }
                other => panic!("expected Quantified for {q}, got {other:?}"),
            }
        }
    }

    #[test]
    fn lowers_multi_hop_quantified_inner() {
        let q = ok("MATCH (a) ((x)-[:KNOWS]->(y)-[:WORKS_AT]->(z)){1,2} (b) RETURN b");
        match &first_pattern(&q).segments.as_ref().unwrap()[0] {
            Segment::Quantified { inner, .. } => {
                assert_eq!(inner.len(), 2);
                assert_eq!(
                    inner[0].0.type_expr,
                    Some(LabelExpr::Atom("KNOWS".to_string()))
                );
                assert_eq!(
                    inner[1].0.type_expr,
                    Some(LabelExpr::Atom("WORKS_AT".to_string()))
                );
            }
            other => panic!("expected Quantified, got {other:?}"),
        }
    }

    #[test]
    fn lowers_hop_then_quantified_mixed() {
        // A plain Cypher hop followed by a GQL quantified group in one pattern:
        // the element order is preserved as [Hop, Quantified].
        let q = ok("MATCH (a:Person)-[:KNOWS]->(m) ((x)-[:KNOWS]->(y)){2} (b) RETURN b");
        let segs = first_pattern(&q).segments.as_ref().unwrap();
        assert_eq!(segs.len(), 2);
        assert!(matches!(segs[0], Segment::Hop(_, _)));
        assert!(matches!(segs[1], Segment::Quantified { .. }));
    }

    #[test]
    fn quantified_rejects_path_variable() {
        let e = err("MATCH p = (a) ((x)-[:R]->(y)){1,2} (b) RETURN p");
        assert!(e.contains("path variable"), "{e}");
    }

    #[test]
    fn quantified_rejects_inner_start_labels() {
        let e = err("MATCH (a) ((x:Person)-[:R]->(y)){1,2} (b) RETURN b");
        assert!(e.contains("first node of a quantified"), "{e}");
    }

    #[test]
    fn bare_pattern_rejects_quantifier() {
        // The quantifier lives only in `match_pattern`; shortestPath/EXISTS/pattern
        // comprehensions use the plain `pattern` rule, so a quantifier there is a
        // syntax error rather than a silently mis-handled segment.
        assert!(parse("MATCH (a),(b) WHERE shortestPath(((x)-[:R]->(y)){1,2}) RETURN a").is_err());
    }

    // ── GQL path restrictors (PR 2) ──────────────────────────────────────────

    #[test]
    fn lowers_path_restrictors() {
        // Each restrictor keyword parses onto the pattern; the chain is otherwise
        // an ordinary variable-length pattern (`segments` stays `None`).
        for (kw, want) in [
            ("WALK", PathRestrictor::Walk),
            ("TRAIL", PathRestrictor::Trail),
            ("ACYCLIC", PathRestrictor::Acyclic),
            ("SIMPLE", PathRestrictor::Simple),
        ] {
            let q = ok(&format!("MATCH {kw} (a)-[:R*1..3]->(b) RETURN b"));
            let p = first_pattern(&q);
            assert_eq!(p.restrictor, Some(want), "restrictor for {kw}");
            assert!(p.segments.is_none(), "restrictor pattern stays ordinary");
            assert_eq!(p.rels.len(), 1);
            assert!(p.rels[0].0.var_length.is_some());
        }
    }

    #[test]
    fn absent_restrictor_is_none() {
        // No restrictor keyword → `None`, which the executor maps onto today's
        // edge-unique (TRAIL) behaviour, so existing queries are untouched.
        let q = ok("MATCH (a)-[:R*1..3]->(b) RETURN b");
        assert_eq!(first_pattern(&q).restrictor, None);
    }

    #[test]
    fn restrictor_lowercase_accepted() {
        // Keywords are case-insensitive like every other keyword terminal.
        let q = ok("match trail (a)-[:R*]->(b) return b");
        assert_eq!(first_pattern(&q).restrictor, Some(PathRestrictor::Trail));
    }

    #[test]
    fn restrictor_does_not_shadow_node_var() {
        // A restrictor only sits at the head of a `match_pattern`, never inside
        // `(…)`, so a node variable spelled `walk` is parsed as a variable, not a
        // restrictor — and the pattern carries no restrictor.
        let q = ok("MATCH (walk)-[:R*]->(b) RETURN b");
        let p = first_pattern(&q);
        assert_eq!(p.start.var.as_deref(), Some("walk"));
        assert_eq!(p.restrictor, None);
    }

    #[test]
    fn restrictor_over_quantified_rejected() {
        // A restrictor over a quantified group is deferred (the group desugars into
        // separate expansions that can't share a uniqueness scope), so it is
        // rejected with a clear message rather than silently ignored.
        let e = err("MATCH TRAIL (a) ((x)-[:R]->(y)){1,2} (b) RETURN b");
        assert!(e.contains("restrictor") && e.contains("quantified"), "{e}");
    }

    // ── GQL shortest-path selectors (PR 3) ───────────────────────────────────

    #[test]
    fn lowers_path_selectors() {
        // Each selector form parses onto the pattern; the chain is otherwise an
        // ordinary variable-length pattern (`segments` stays `None`).
        for (kw, want) in [
            ("ANY SHORTEST", PathSelector::AnyShortest),
            ("ALL SHORTEST", PathSelector::AllShortest),
            ("SHORTEST 3", PathSelector::ShortestK(3)),
        ] {
            let q = ok(&format!("MATCH {kw} (a)-[:R*]->(b) RETURN b"));
            let p = first_pattern(&q);
            assert_eq!(p.selector, Some(want), "selector for {kw}");
            assert!(p.segments.is_none(), "selector pattern stays ordinary");
            assert_eq!(p.rels.len(), 1);
        }
    }

    #[test]
    fn absent_selector_is_none() {
        // No selector keyword → `None`: the pattern runs the ordinary matcher.
        let q = ok("MATCH (a)-[:R*]->(b) RETURN b");
        assert_eq!(first_pattern(&q).selector, None);
    }

    #[test]
    fn selector_lowercase_accepted() {
        // Keywords are case-insensitive like every other keyword terminal.
        let q = ok("match any shortest (a)-[:R*]->(b) return b");
        assert_eq!(first_pattern(&q).selector, Some(PathSelector::AnyShortest));
    }

    #[test]
    fn selector_with_path_var_follows_prefix() {
        // The path-variable assignment follows the selector prefix
        // (`SELECTOR p = …`), consistent with the restrictor placement (PR 2).
        let q = ok("MATCH ALL SHORTEST p = (a)-[:R*]->(b) RETURN p");
        let p = first_pattern(&q);
        assert_eq!(p.selector, Some(PathSelector::AllShortest));
        assert_eq!(p.path_var.as_deref(), Some("p"));
    }

    #[test]
    fn selector_does_not_shadow_node_var() {
        // A selector only sits at the head of a `match_pattern`, never inside `(…)`,
        // so a node variable spelled `shortest` parses as a variable, not a selector.
        let q = ok("MATCH (shortest)-[:R*]->(b) RETURN b");
        let p = first_pattern(&q);
        assert_eq!(p.start.var.as_deref(), Some("shortest"));
        assert_eq!(p.selector, None);
    }

    #[test]
    fn selector_zero_k_rejected() {
        // `SHORTEST 0` is meaningless; rejected at lowering with a clear message.
        let e = err("MATCH SHORTEST 0 (a)-[:R*]->(b) RETURN b");
        assert!(e.contains("positive count"), "{e}");
    }

    #[test]
    fn selector_over_quantified_rejected() {
        // A selector over a quantified group is deferred, like the restrictor: the
        // group desugars into separate expansions, not a single var-length walk.
        let e = err("MATCH ANY SHORTEST (a) ((x)-[:R]->(y)){1,2} (b) RETURN b");
        assert!(e.contains("selector") && e.contains("quantified"), "{e}");
    }

    #[test]
    fn all_shortest_paths_not_supported() {
        // allShortestPaths is deferred: it is not in the grammar, so its `(…)` body
        // parses as ordinary function arguments (the inner pattern as a pattern
        // predicate) and the name is rejected as an unknown function at eval time.
        let q = ok("MATCH (a), (b) RETURN allShortestPaths((a)-[*]->(b))");
        assert!(matches!(
            &q.head.ret.body.items[0].expr,
            Expr::Function { name, .. } if name == "allShortestPaths"
        ));
    }

    #[test]
    fn shortest_path_fn_in_match_desugars_to_any_shortest() {
        // openCypher `MATCH p = shortestPath((a)-[:R*]->(b))` (Neo4j/Memgraph spelling)
        // desugars onto the GQL `ANY SHORTEST` selector + bound path variable.
        let q = ok("MATCH p = shortestPath((a)-[:R*]->(b)) RETURN length(p)");
        let p = first_pattern(&q);
        assert_eq!(p.selector, Some(PathSelector::AnyShortest));
        assert_eq!(p.path_var.as_deref(), Some("p"));
        assert_eq!(p.start.var.as_deref(), Some("a"));
    }

    #[test]
    fn all_shortest_paths_fn_in_match_desugars_to_all_shortest() {
        let q = ok("MATCH p = allShortestPaths((a)-[:R*]->(b)) RETURN p");
        let p = first_pattern(&q);
        assert_eq!(p.selector, Some(PathSelector::AllShortest));
        assert_eq!(p.path_var.as_deref(), Some("p"));
    }

    #[test]
    fn shortest_path_fn_path_var_optional_and_case_insensitive() {
        // The bound path variable is optional, and the keyword is case-insensitive.
        let q = ok("match shortestpath((a)-[:r*]->(b)) return a");
        let p = first_pattern(&q);
        assert_eq!(p.selector, Some(PathSelector::AnyShortest));
        assert_eq!(p.path_var, None);
    }

    #[test]
    fn shortest_path_fn_does_not_shadow_node_var() {
        // `shortestPath` only triggers in MATCH-call position; as a node variable
        // inside `(…)` it stays an ordinary variable.
        let q = ok("MATCH (shortestPath)-[:R]->(b) RETURN b");
        let p = first_pattern(&q);
        assert_eq!(p.start.var.as_deref(), Some("shortestPath"));
        assert_eq!(p.selector, None);
    }

    // ── GQL label boolean expressions (PR 4) ─────────────────────────────────

    fn atom(s: &str) -> LabelExpr {
        LabelExpr::Atom(s.to_string())
    }

    fn node_label_expr(q: &str) -> LabelExpr {
        first_pattern(&ok(q))
            .start
            .label_expr
            .clone()
            .expect("node carries a label expression")
    }

    fn rel_type_expr(q: &str) -> LabelExpr {
        first_pattern(&ok(q)).rels[0]
            .0
            .type_expr
            .clone()
            .expect("relationship carries a type expression")
    }

    #[test]
    fn lowers_label_and_with_colon_sugar() {
        // `:A&B` and the classic `:A:B` sugar lower to the SAME And tree — the `:`
        // separator is just AND. A regression that lowered the colon chain to Or
        // would change this equality.
        let want = LabelExpr::And(Box::new(atom("Person")), Box::new(atom("Employee")));
        assert_eq!(node_label_expr("MATCH (n:Person&Employee) RETURN n"), want);
        assert_eq!(node_label_expr("MATCH (n:Person:Employee) RETURN n"), want);
    }

    #[test]
    fn lowers_label_or_on_node_and_reltype() {
        // `|` lowers to Or for node labels; the pre-GQL rel-type alternation
        // `:T1|T2` lowers to the same Or, so the alternation sugar is preserved.
        assert_eq!(
            node_label_expr("MATCH (n:Person|Company) RETURN n"),
            LabelExpr::Or(Box::new(atom("Person")), Box::new(atom("Company"))),
        );
        assert_eq!(
            rel_type_expr("MATCH (a)-[:KNOWS|WORKS_AT]->(b) RETURN b"),
            LabelExpr::Or(Box::new(atom("KNOWS")), Box::new(atom("WORKS_AT"))),
        );
    }

    #[test]
    fn lowers_label_negation() {
        assert_eq!(
            node_label_expr("MATCH (n:!Person) RETURN n"),
            LabelExpr::Not(Box::new(atom("Person"))),
        );
        assert_eq!(
            rel_type_expr("MATCH (a)-[:!KNOWS]->(b) RETURN b"),
            LabelExpr::Not(Box::new(atom("KNOWS"))),
        );
    }

    #[test]
    fn label_expr_precedence_not_over_and_over_or() {
        // `!` binds tighter than `&`, which binds tighter than `|`:
        //   !A&B   ≡ (!A)&B
        //   A|B&C  ≡ A|(B&C)
        assert_eq!(
            node_label_expr("MATCH (n:!A&B) RETURN n"),
            LabelExpr::And(
                Box::new(LabelExpr::Not(Box::new(atom("A")))),
                Box::new(atom("B")),
            ),
        );
        assert_eq!(
            node_label_expr("MATCH (n:A|B&C) RETURN n"),
            LabelExpr::Or(
                Box::new(atom("A")),
                Box::new(LabelExpr::And(Box::new(atom("B")), Box::new(atom("C")))),
            ),
        );
    }

    #[test]
    fn label_parens_override_precedence() {
        // Parentheses force the OR first, against the default precedence.
        assert_eq!(
            node_label_expr("MATCH (n:(A|B)&C) RETURN n"),
            LabelExpr::And(
                Box::new(LabelExpr::Or(Box::new(atom("A")), Box::new(atom("B")))),
                Box::new(atom("C")),
            ),
        );
    }

    #[test]
    fn absent_label_is_none() {
        // No label constraint ⇒ `None` (any node), so the hot path is untouched.
        assert_eq!(
            first_pattern(&ok("MATCH (n) RETURN n")).start.label_expr,
            None
        );
        assert_eq!(
            first_pattern(&ok("MATCH (a)-[r]->(b) RETURN b")).rels[0]
                .0
                .type_expr,
            None,
        );
    }

    // ── GQL PR 5 — `FOR alias IN expr` lowers onto UnwindClause ───────────────

    #[test]
    fn for_lowers_to_unwind_clause() {
        // GQL `FOR x IN [1,2,3]` is exactly `UNWIND [1,2,3] AS x` after the parser:
        // same UnwindClause, just the surface operand order reversed.
        let q = ok("FOR x IN [1, 2, 3] RETURN x");
        match &q.head.reading[0] {
            Clause::Unwind(uc) => {
                assert_eq!(uc.var, "x");
                assert!(matches!(uc.expr, Expr::List(_)));
            }
            other => panic!("expected an Unwind clause, got {other:?}"),
        }

        // Identical AST to the Cypher spelling — the two parse to the same clause.
        let from_for = ok("FOR x IN [1, 2, 3] RETURN x");
        let from_unwind = ok("UNWIND [1, 2, 3] AS x RETURN x");
        assert_eq!(from_for.head.reading, from_unwind.head.reading);
    }

    #[test]
    fn cast_lowers_onto_conversion_functions() {
        // The RETURN projection expression of `q`'s sole RETURN item.
        fn return_expr(q: &Query) -> &Expr {
            &q.head.ret.body.items[0].expr
        }

        // CAST(x AS <type>) lowers to the matching to*/temporal function call; each
        // spelling (and its alias) maps to the same function name.
        for (q, func) in [
            ("RETURN CAST('7' AS INTEGER)", "toInteger"),
            ("RETURN CAST('7' AS INT)", "toInteger"),
            ("RETURN CAST(1 AS FLOAT)", "toFloat"),
            ("RETURN CAST(1 AS DOUBLE)", "toFloat"),
            ("RETURN CAST(1 AS STRING)", "toString"),
            ("RETURN CAST('true' AS BOOLEAN)", "toBoolean"),
            ("RETURN CAST('2024-01-01' AS DATE)", "date"),
        ] {
            match return_expr(&ok(q)) {
                Expr::Function { name, args, .. } => {
                    assert_eq!(name, func, "for {q:?}");
                    assert!(matches!(args, FuncArgs::Args(a) if a.len() == 1));
                }
                other => panic!("expected a Function for {q:?}, got {other:?}"),
            }
        }

        // CAST(x AS INTEGER) is the identical AST to toInteger(x) — pure sugar.
        assert_eq!(
            ok("RETURN CAST('7' AS INTEGER)").head.ret.body.items,
            ok("RETURN toInteger('7')").head.ret.body.items,
        );
    }

    // ── Phase 12 — CALL { … } subquery ───────────────────────────────────────

    #[test]
    fn lowers_call_subquery() {
        // A returning subquery with a simple import WITH: lowers to a CallSubquery
        // clause whose head imports `p` and returns one column.
        let q = ok("MATCH (p) CALL { WITH p RETURN p.age AS age } RETURN p, age");
        let Clause::CallSubquery(cs) = &q.head.reading[1] else {
            panic!(
                "expected a CallSubquery clause, got {:?}",
                q.head.reading[1]
            );
        };
        assert!(cs.returning);
        assert_eq!(cs.imports, vec![Imports::Named(vec!["p".to_string()])]);
        assert!(cs.inner.tail.is_empty());

        // No leading WITH → no imports; outer variables are invisible inside.
        let q = ok("UNWIND [1, 2] AS x CALL { UNWIND [3, 4] AS y RETURN y } RETURN x, y");
        let Clause::CallSubquery(cs) = &q.head.reading[1] else {
            panic!("expected a CallSubquery clause");
        };
        assert_eq!(cs.imports, vec![Imports::None]);
        assert!(cs.returning);
    }

    #[test]
    fn lowers_unit_and_union_subqueries() {
        // Unit subquery (no inner RETURN): returning is false.
        let q = ok("WITH 1 AS a CALL { MATCH (p:Person) } RETURN a");
        let Clause::CallSubquery(cs) = &q.head.reading[1] else {
            panic!("expected a CallSubquery clause");
        };
        assert!(!cs.returning);

        // UNION inside the subquery: two branches, one union-all flag, per-branch
        // imports.
        let q = ok(
            "MATCH (p) CALL { WITH p RETURN p.name AS x UNION WITH p RETURN p.city AS x } RETURN x",
        );
        let Clause::CallSubquery(cs) = &q.head.reading[1] else {
            panic!("expected a CallSubquery clause");
        };
        assert!(cs.returning);
        assert_eq!(cs.inner.tail.len(), 1);
        assert!(!cs.inner.tail[0].0, "UNION (not ALL) is distinct");
        assert_eq!(cs.imports.len(), 2);
    }

    #[test]
    fn call_subquery_import_validation() {
        // Only simple variable references may be imported (FalkorDB
        // _ValidateCallInitialWith). Each of these violates the rule.
        for q in [
            "WITH 1 AS a CALL { WITH a + 1 AS b RETURN b } RETURN b",
            "WITH 1 AS a CALL { WITH a AS b RETURN b } RETURN b",
            "WITH 1 AS a CALL { WITH a LIMIT 5 RETURN a } RETURN a",
            "WITH 1 AS a CALL { WITH a ORDER BY a RETURN a } RETURN a",
            "WITH 1 AS a CALL { WITH a WHERE a > 5 RETURN a } RETURN a",
            "WITH 1 AS a CALL { WITH a SKIP 5 RETURN a } RETURN a",
        ] {
            let e = err(q);
            assert!(
                e.contains("simple references to outside variables"),
                "for {q:?} expected import error, got: {e}"
            );
        }
    }

    #[test]
    fn call_subquery_rejects_writes() {
        // A write clause inside the subquery is rejected as read-only at lowering.
        let e = err("CALL { MATCH (n) SET n.x = 1 RETURN n } RETURN n");
        assert!(
            e.contains("read-only"),
            "expected read-only rejection, got: {e}"
        );
        // A bare CREATE subquery likewise rejects (the `CALL {` carve-out does not
        // let writes through).
        let e = err("WITH 1 AS a CALL { CREATE (n:N) RETURN n } RETURN a");
        assert!(e.contains("read-only"), "got: {e}");
    }

    // Phase 1b — the non-determinism detector that gates result caching.
    #[test]
    fn detects_nondeterministic_functions() {
        // Calls to rand/randomUUID/timestamp anywhere → non-deterministic.
        for q in [
            "RETURN rand()",
            "RETURN randomUUID()",
            "RETURN timestamp()",
            "RETURN TIMESTAMP()",                        // case-insensitive
            "MATCH (n) WHERE n.age < rand() RETURN n",   // buried in WHERE
            "MATCH (n) RETURN n ORDER BY rand()",        // in ORDER BY
            "RETURN [x IN [1,2,3] | timestamp()]",       // in a comprehension
            "MATCH (n {created: timestamp()}) RETURN n", // in a pattern prop map
            "CALL { RETURN rand() AS r } RETURN r",      // in a subquery
        ] {
            assert!(
                is_nondeterministic(&ok(q)),
                "expected non-deterministic: {q:?}"
            );
        }

        // Deterministic queries — note the string literal `'timestamp()'` is NOT a
        // call (the AST walk beats a naive substring match).
        for q in [
            "RETURN 1",
            "MATCH (n:Person) RETURN n.name",
            "RETURN date('2020-01-01')",
            "RETURN 'timestamp()' AS s",
            "MATCH (n) WHERE n.timestamp > 5 RETURN n", // property named `timestamp`
        ] {
            assert!(
                !is_nondeterministic(&ok(q)),
                "expected deterministic: {q:?}"
            );
        }
    }
}
