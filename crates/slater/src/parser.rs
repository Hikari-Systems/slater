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

    #[derive(Debug, Clone, PartialEq)]
    pub enum Clause {
        Match(MatchClause),
        With(WithClause),
        VectorCall(VectorCallClause),
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
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct NodePat {
        pub var: Option<String>,
        pub labels: Vec<String>,
        pub props: Vec<(String, Expr)>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct RelPat {
        pub var: Option<String>,
        pub dir: Direction,
        pub types: Vec<String>,
        pub var_length: Option<VarLength>,
        pub props: Vec<(String, Expr)>,
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
            other => bail!("internal: unexpected single_query child {other:?}"),
        }
    }
    Ok(SingleQuery {
        reading,
        ret: ret.ok_or_else(|| anyhow::anyhow!("query has no RETURN"))?,
    })
}

fn lower_reading_clause(pair: Pair<Rule>) -> Result<Clause> {
    let inner = only_child(pair)?;
    match inner.as_rule() {
        Rule::match_clause => Ok(Clause::Match(lower_match_clause(inner)?)),
        Rule::with_clause => Ok(Clause::With(lower_with_clause(inner)?)),
        Rule::vector_call_clause => Ok(Clause::VectorCall(lower_vector_call(inner)?)),
        other => bail!("internal: unexpected reading clause {other:?}"),
    }
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
            Rule::pattern => patterns.push(lower_pattern(child)?),
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

fn lower_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut path_var = None;
    let mut nodes = Vec::new();
    let mut rels = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::path_var => path_var = Some(ident_text(child)?),
            Rule::node_pattern => nodes.push(lower_node_pattern(child)?),
            Rule::rel_pattern => rels.push(lower_rel_pattern(child)?),
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
    })
}

fn lower_node_pattern(pair: Pair<Rule>) -> Result<NodePat> {
    let mut var = None;
    let mut labels = Vec::new();
    let mut props = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::var => var = Some(ident_text(only_child(child)?)?),
            Rule::labels => labels = lower_labels(child)?,
            Rule::prop_map => props = lower_prop_map(child)?,
            other => bail!("internal: unexpected node child {other:?}"),
        }
    }
    Ok(NodePat { var, labels, props })
}

fn lower_rel_pattern(pair: Pair<Rule>) -> Result<RelPat> {
    let mut left = false;
    let mut right = false;
    let mut var = None;
    let mut types = Vec::new();
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
                        Rule::rel_types => {
                            for t in d.into_inner() {
                                if t.as_rule() == Rule::type_name {
                                    types.push(ident_text(t)?);
                                }
                            }
                        }
                        Rule::var_length => var_length = Some(lower_var_length(d)?),
                        Rule::prop_map => props = lower_prop_map(d)?,
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
        types,
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
        p.as_str()
            .parse::<u32>()
            .map_err(|e| anyhow::anyhow!("bad var-length bound {:?}: {e}", p.as_str()))
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

fn lower_labels(pair: Pair<Rule>) -> Result<Vec<String>> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::label_name)
        .map(ident_text)
        .collect()
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
                left = Expr::Compare(cmp_op(op_pair.as_str()), Box::new(left), Box::new(right));
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
        Rule::case_expr => lower_case(inner),
        Rule::map_projection => lower_map_projection(inner),
        Rule::list_comprehension => lower_list_predicate(inner),
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

fn lower_literal(pair: Pair<Rule>) -> Result<Expr> {
    let inner = only_child(pair)?;
    let v = match inner.as_rule() {
        Rule::integer => Value::Int(
            inner
                .as_str()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("bad integer {:?}: {e}", inner.as_str()))?,
        ),
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
            | Rule::kw_call
            | Rule::kw_yield
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

/// Extract an identifier's text, stripping surrounding backticks if quoted.
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
    let s = p.as_str();
    Ok(s.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(s)
        .to_string())
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
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('"') => out.push('"'),
            Some(other) => out.push(other),
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
        ];
        for q in corpus {
            ok(q);
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
            ("UNWIND [1, 2, 3] AS x RETURN x", "UNWIND"),
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
        assert_eq!(p.start.labels, vec!["Person".to_string()]);
        assert_eq!(p.rels.len(), 1);
        let (rel, end) = &p.rels[0];
        assert_eq!(rel.dir, Direction::Outgoing);
        assert_eq!(rel.types, vec!["KNOWS".to_string()]);
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
    fn string_literals_unescape() {
        let q = ok(r#"RETURN 'a\'b\nc' AS s"#);
        assert_eq!(
            q.head.ret.body.items[0].expr,
            Expr::Literal(Value::Str("a'b\nc".to_string()))
        );
    }
}
