// SPDX-License-Identifier: Apache-2.0
//! Primitive-Cypher parser and the streaming statement reader.
//!
//! [`StatementReader`] splits a dump script into individual statements on
//! top-level `;` (respecting string literals, which may span many lines for large
//! markdown property values) without holding the whole file in memory.
//! [`parse_statement`] turns one statement string into a typed [`Statement`].

use std::io::BufRead;

use anyhow::{anyhow, bail, Context, Result};
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use graph_format::ids::Value;

use crate::model::{
    EdgeOverwriteStmt, EdgeStmt, Entity, NodeMatch, NodeOverwriteStmt, NodeStmt, RangeIndexStmt,
    SetExpr, Statement, VectorIndexStmt,
};

#[derive(Parser)]
#[grammar = "primitive_cypher.pest"]
struct PrimitiveCypher;

// ---------------------------------------------------------------------------
// Streaming statement reader
// ---------------------------------------------------------------------------

/// Splits a dump script into statements on top-level `;`. Operates on bytes: the
/// delimiters it tracks (`;`, `'`, `"`, `` ` ``, `\`) are all ASCII, and UTF-8
/// continuation bytes are always `>= 0x80`, so byte-level scanning never splits a
/// multibyte character or mistakes one for a delimiter.
///
// DESIGN: dump scripts can be very large (multi-paragraph markdown text fields),
// so we never slurp the whole file — we pull bytes from a `BufRead` and emit one
// statement at a time. A `;` is a separator only outside a string literal *and*
// outside a backtick-quoted identifier; inside a literal (single- or double-quoted,
// with `\` escapes) or inside a quoted name (no escapes — a doubled backtick simply
// closes and re-opens one) it is ordinary text.
pub struct StatementReader<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    in_string: Option<u8>,
    /// Inside a backtick-quoted identifier (`` `a b` ``), where `;`/quotes are text.
    in_ident: bool,
    escaped: bool,
    done: bool,
}

impl<R: BufRead> StatementReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            in_string: None,
            in_ident: false,
            escaped: false,
            done: false,
        }
    }

    /// Return the next non-empty statement (trimmed), or `None` at end of input.
    pub fn next_statement(&mut self) -> Result<Option<String>> {
        'outer: loop {
            // Drain whatever the BufReader already has, byte by byte, until a
            // top-level `;` completes a statement.
            let available = self.reader.fill_buf().context("read dump script")?;
            if available.is_empty() {
                // EOF: emit any trailing statement without a terminating `;`.
                if self.done {
                    return Ok(None);
                }
                self.done = true;
                return self.take_statement();
            }
            let mut consumed = 0;
            for &b in available {
                consumed += 1;
                if let Some(quote) = self.in_string {
                    if self.escaped {
                        self.escaped = false;
                    } else if b == b'\\' {
                        self.escaped = true;
                    } else if b == quote {
                        self.in_string = None;
                    }
                    self.buf.push(b);
                } else if self.in_ident {
                    // A quoted name has no `\` escapes: the only terminator is the
                    // closing backtick. (A doubled backtick closes and immediately
                    // re-opens, which keeps `;`/quotes inside it text either way.)
                    if b == b'`' {
                        self.in_ident = false;
                    }
                    self.buf.push(b);
                } else {
                    match b {
                        b'\'' | b'"' => {
                            self.in_string = Some(b);
                            self.buf.push(b);
                        }
                        b'`' => {
                            self.in_ident = true;
                            self.buf.push(b);
                        }
                        b';' => {
                            // Statement boundary.
                            self.reader.consume(consumed);
                            if let Some(s) = self.take_statement()? {
                                return Ok(Some(s));
                            }
                            // Empty statement (e.g. stray `;`): keep scanning from the
                            // next byte. Iterative on purpose — recursing here grew the
                            // stack one frame per empty statement, so a script of many
                            // stray `;` could overflow (only tail-call elimination, which
                            // Rust does not guarantee, kept it from crashing).
                            continue 'outer;
                        }
                        _ => self.buf.push(b),
                    }
                }
            }
            self.reader.consume(consumed);
        }
    }

    fn take_statement(&mut self) -> Result<Option<String>> {
        let bytes = std::mem::take(&mut self.buf);
        // Strict UTF-8: `from_utf8_lossy` would silently replace invalid bytes with
        // U+FFFD, corrupting a property value (or a quoted identifier) rather than
        // surfacing the malformed input. A dump is text; reject non-UTF-8 loudly.
        let s = std::str::from_utf8(&bytes).context("dump statement is not valid UTF-8")?;
        let trimmed = s.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    }
}

// ---------------------------------------------------------------------------
// Statement parsing
// ---------------------------------------------------------------------------

/// Parse one statement string (no trailing `;`) into a typed [`Statement`], using the
/// default `__dump_id__` identity field for legacy edge endpoints.
pub fn parse_statement(input: &str) -> Result<Statement> {
    parse_statement_with_id_field(input, "__dump_id__")
}

/// Parse one statement, reading legacy `MATCH … CREATE` edge endpoints by the
/// configurable identity property `id_field` (`--pk`) instead of the hardcoded
/// `__dump_id__`. Only affects `edge_create`; all other forms are field-agnostic.
pub fn parse_statement_with_id_field(input: &str, id_field: &str) -> Result<Statement> {
    let mut pairs =
        PrimitiveCypher::parse(Rule::statement, input).map_err(|e| anyhow!("parse error: {e}"))?;
    // statement -> stmt -> <one concrete form>
    let statement = pairs.next().ok_or_else(|| anyhow!("empty parse"))?;
    let stmt = first_inner(statement, Rule::stmt)?;
    let form = stmt
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty stmt"))?;
    match form.as_rule() {
        Rule::node_create => parse_node_create(form),
        Rule::edge_create => parse_edge_create(form, id_field),
        Rule::node_overwrite => parse_node_overwrite(form),
        Rule::edge_overwrite => parse_edge_overwrite(form),
        Rule::create_index => parse_create_index(form),
        Rule::vector_index_call => parse_vector_index(form, ArgOrder::Call),
        Rule::vector_index_helper => parse_vector_index(form, ArgOrder::Helper),
        Rule::ignored => Ok(Statement::Ignored),
        other => bail!("unexpected statement rule {other:?}"),
    }
}

fn first_inner(pair: Pair<Rule>, want: Rule) -> Result<Pair<Rule>> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("expected inner {want:?}"))?;
    Ok(inner)
}

fn parse_node_create(pair: Pair<Rule>) -> Result<Statement> {
    let node_pattern = first_inner(pair, Rule::node_pattern)?;
    let (_var, labels, props) = parse_node_pattern(node_pattern)?;
    Ok(Statement::Node(NodeStmt { labels, props }))
}

fn parse_edge_create(pair: Pair<Rule>, id_field: &str) -> Result<Statement> {
    // edge_create = node_pattern ~ node_pattern ~ rel_create
    let mut endpoints: Vec<(String, i64)> = Vec::new();
    let mut rel: Option<Pair<Rule>> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::node_pattern => {
                let (var, _labels, props) = parse_node_pattern(child)?;
                let var = var.ok_or_else(|| {
                    anyhow!("edge MATCH endpoint must bind a variable to reference in CREATE")
                })?;
                let dump_id = props
                    .iter()
                    .find(|(k, _)| k == id_field)
                    .and_then(|(_, v)| as_int(v))
                    .ok_or_else(|| anyhow!("edge MATCH endpoint missing integer {id_field}"))?;
                endpoints.push((var, dump_id));
            }
            Rule::rel_create => rel = Some(child),
            _ => {}
        }
    }
    let rel = rel.ok_or_else(|| anyhow!("edge statement missing CREATE relationship"))?;

    // rel_create = "(" var ")" "-" "[" rel_detail "]" "->" "(" var ")"
    let mut src_var = None;
    let mut dst_var = None;
    let mut reltype = String::new();
    let mut props: Vec<(String, Value)> = Vec::new();
    for child in rel.into_inner() {
        match child.as_rule() {
            Rule::var => {
                if src_var.is_none() {
                    src_var = Some(child.as_str().to_string());
                } else {
                    dst_var = Some(child.as_str().to_string());
                }
            }
            Rule::rel_detail => {
                for d in child.into_inner() {
                    match d.as_rule() {
                        Rule::reltype => reltype = unquote_key(d.as_str()),
                        Rule::prop_map => props = parse_prop_map(d)?,
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    let src_var = src_var.ok_or_else(|| anyhow!("relationship missing source variable"))?;
    let dst_var = dst_var.ok_or_else(|| anyhow!("relationship missing target variable"))?;

    let lookup = |v: &str| -> Result<i64> {
        endpoints
            .iter()
            .find(|(name, _)| name == v)
            .map(|(_, id)| *id)
            .ok_or_else(|| anyhow!("relationship variable '{v}' not bound in MATCH"))
    };
    Ok(Statement::Edge(EdgeStmt {
        src_dump_id: lookup(&src_var)?,
        dst_dump_id: lookup(&dst_var)?,
        reltype,
        props,
    }))
}

/// Extract a node match (identity `label` + any additional labels + one `{key: value}`)
/// from a matched pattern. At least one label and exactly one match property are
/// required; the first label is the identity (it locates/creates the node), the rest are
/// written alongside it (`MERGE (n:Ident:Other {k:v})`).
fn node_match_from_pattern(pair: Pair<Rule>) -> Result<NodeMatch> {
    let (_var, labels, props) = parse_node_pattern(pair)?;
    if labels.is_empty() {
        bail!("overwrite match pattern must have at least one label");
    }
    if props.len() != 1 {
        bail!(
            "overwrite match pattern must have exactly one {{key: value}} entry, got {}",
            props.len()
        );
    }
    let mut labels = labels.into_iter();
    let label = labels.next().unwrap();
    let extra_labels: Vec<String> = labels.collect();
    let (key, value) = props.into_iter().next().unwrap();
    Ok(NodeMatch {
        label,
        extra_labels,
        key,
        value,
    })
}

/// Parse a `set_clause` (`SET v.k = rhs, …`) into `(key, expr)` assignments, where
/// each `rhs` is a literal, a same-node property reference, or a pure scalar
/// function call. The binding variable (`v`) is ignored — v1 patterns bind a
/// single entity.
fn parse_set_clause(pair: Pair<Rule>) -> Result<Vec<(String, SetExpr)>> {
    let mut out = Vec::new();
    for assign in pair.into_inner() {
        if assign.as_rule() != Rule::set_assign {
            continue;
        }
        let mut key: Option<String> = None;
        let mut expr: Option<SetExpr> = None;
        for c in assign.into_inner() {
            match c.as_rule() {
                Rule::key => key = Some(unquote_key(c.as_str())),
                Rule::set_expr => expr = Some(parse_set_expr(c)?),
                _ => {}
            }
        }
        let key = key.ok_or_else(|| anyhow!("SET assignment missing property key"))?;
        let expr = expr.ok_or_else(|| anyhow!("SET assignment missing value"))?;
        out.push((key, expr));
    }
    if out.is_empty() {
        bail!("SET clause must assign at least one property");
    }
    Ok(out)
}

/// Like [`parse_set_clause`] but for contexts that accept only literal values
/// (edge SET, overlay patches). A function call or property reference is an error.
fn parse_set_clause_literal(pair: Pair<Rule>) -> Result<Vec<(String, Value)>> {
    parse_set_clause(pair)?
        .into_iter()
        .map(|(k, e)| match e {
            SetExpr::Lit(v) => Ok((k, v)),
            _ => bail!(
                "edge SET supports only literal values, not functions, operators, CASE, \
                 or property references (for `{k}`)"
            ),
        })
        .collect()
}

/// Whether a parse rule is a node in the `set_expr` precedence chain (as opposed to
/// an operator token or a keyword). Used to skip operators when collecting operands.
fn is_expr_rule(r: Rule) -> bool {
    matches!(
        r,
        Rule::set_expr
            | Rule::or_expr
            | Rule::and_expr
            | Rule::not_expr
            | Rule::comparison
            | Rule::add_expr
            | Rule::mul_expr
            | Rule::primary
            | Rule::paren
            | Rule::case_expr
            | Rule::func_call
            | Rule::value
            | Rule::prop_ref
    )
}

/// Parse any node in the `set_expr` precedence chain into a [`SetExpr`]. Single-child
/// chain levels (no operator at that precedence) pass straight through, so a bare
/// literal yields `SetExpr::Lit` with no wrapper nodes.
fn parse_set_expr(pair: Pair<Rule>) -> Result<SetExpr> {
    match pair.as_rule() {
        // Thin wrappers carrying exactly one inner expression.
        Rule::set_expr | Rule::primary | Rule::paren | Rule::case_subject | Rule::else_clause => {
            let inner = pair
                .into_inner()
                .find(|p| is_expr_rule(p.as_rule()))
                .ok_or_else(|| anyhow!("empty SET expression"))?;
            parse_set_expr(inner)
        }
        Rule::or_expr => parse_bool_chain(pair, true),
        Rule::and_expr => parse_bool_chain(pair, false),
        Rule::not_expr => parse_not(pair),
        Rule::comparison => parse_comparison(pair),
        Rule::add_expr | Rule::mul_expr => parse_arith_chain(pair),
        Rule::case_expr => parse_case(pair),
        Rule::func_call => {
            let mut name: Option<String> = None;
            let mut args = Vec::new();
            for c in pair.into_inner() {
                match c.as_rule() {
                    Rule::fn_name => name = Some(c.as_str().to_string()),
                    Rule::set_expr => args.push(parse_set_expr(c)?),
                    _ => {}
                }
            }
            let name = name.ok_or_else(|| anyhow!("function call missing name"))?;
            Ok(SetExpr::Func { name, args })
        }
        Rule::value => Ok(SetExpr::Lit(parse_value(pair)?)),
        Rule::prop_ref => {
            // prop_ref = var "." key ; keep the key, drop the variable.
            let key = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::key)
                .map(|p| unquote_key(p.as_str()))
                .ok_or_else(|| anyhow!("property reference missing key"))?;
            Ok(SetExpr::Prop(key))
        }
        other => bail!("unexpected SET expression rule {other:?}"),
    }
}

/// Left-fold an `or_expr` / `and_expr` (`a OR b OR c`) into nested `Or`/`And`. With a
/// single operand (no operator at this level) returns it directly.
fn parse_bool_chain(pair: Pair<Rule>, is_or: bool) -> Result<SetExpr> {
    let mut operands = pair.into_inner().filter(|p| is_expr_rule(p.as_rule()));
    let first = operands
        .next()
        .ok_or_else(|| anyhow!("empty boolean expression"))?;
    let mut acc = parse_set_expr(first)?;
    for next in operands {
        let rhs = parse_set_expr(next)?;
        acc = if is_or {
            SetExpr::Or(Box::new(acc), Box::new(rhs))
        } else {
            SetExpr::And(Box::new(acc), Box::new(rhs))
        };
    }
    Ok(acc)
}

/// `not_expr = kw_not* ~ comparison` → wrap the operand in `Not` once per `NOT`.
fn parse_not(pair: Pair<Rule>) -> Result<SetExpr> {
    let mut nots = 0usize;
    let mut operand: Option<Pair<Rule>> = None;
    for c in pair.into_inner() {
        match c.as_rule() {
            Rule::kw_not => nots += 1,
            r if is_expr_rule(r) => operand = Some(c),
            _ => {}
        }
    }
    let operand = operand.ok_or_else(|| anyhow!("NOT missing operand"))?;
    let mut e = parse_set_expr(operand)?;
    for _ in 0..nots {
        e = SetExpr::Not(Box::new(e));
    }
    Ok(e)
}

/// `comparison = add_expr ~ (cmp_op ~ add_expr)?` → `Cmp` when the operator is
/// present, else the lone operand.
fn parse_comparison(pair: Pair<Rule>) -> Result<SetExpr> {
    let mut inner = pair.into_inner();
    let left = inner.next().ok_or_else(|| anyhow!("empty comparison"))?;
    let mut acc = parse_set_expr(left)?;
    if let Some(op_pair) = inner.next() {
        let op = parse_cmp_op(op_pair.as_str())?;
        let right = inner
            .next()
            .ok_or_else(|| anyhow!("comparison missing right operand"))?;
        acc = SetExpr::Cmp {
            op,
            l: Box::new(acc),
            r: Box::new(parse_set_expr(right)?),
        };
    }
    Ok(acc)
}

/// `add_expr`/`mul_expr` = operand (op operand)* → left-fold into `BinOp`.
fn parse_arith_chain(pair: Pair<Rule>) -> Result<SetExpr> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| anyhow!("empty arithmetic expression"))?;
    let mut acc = parse_set_expr(first)?;
    while let Some(op_pair) = inner.next() {
        let op = parse_bin_op(op_pair.as_str())?;
        let rhs = inner
            .next()
            .ok_or_else(|| anyhow!("operator `{}` missing right operand", op_pair.as_str()))?;
        acc = SetExpr::BinOp {
            op,
            l: Box::new(acc),
            r: Box::new(parse_set_expr(rhs)?),
        };
    }
    Ok(acc)
}

/// Parse a `case_expr` into [`SetExpr::Case`].
fn parse_case(pair: Pair<Rule>) -> Result<SetExpr> {
    let mut subject = None;
    let mut whens = Vec::new();
    let mut els = None;
    for c in pair.into_inner() {
        match c.as_rule() {
            Rule::case_subject => subject = Some(Box::new(parse_set_expr(c)?)),
            Rule::when_clause => {
                let mut parts = c.into_inner().filter(|p| is_expr_rule(p.as_rule()));
                let cond = parts
                    .next()
                    .ok_or_else(|| anyhow!("WHEN missing condition"))?;
                let then = parts
                    .next()
                    .ok_or_else(|| anyhow!("WHEN missing THEN value"))?;
                whens.push((parse_set_expr(cond)?, parse_set_expr(then)?));
            }
            Rule::else_clause => els = Some(Box::new(parse_set_expr(c)?)),
            _ => {} // kw_case / kw_end markers
        }
    }
    if whens.is_empty() {
        bail!("CASE expression must have at least one WHEN");
    }
    Ok(SetExpr::Case {
        subject,
        whens,
        els,
    })
}

fn parse_cmp_op(s: &str) -> Result<slater_scalar::CmpOp> {
    use slater_scalar::CmpOp::*;
    Ok(match s {
        "=" => Eq,
        "<>" => Ne,
        "<" => Lt,
        "<=" => Le,
        ">" => Gt,
        ">=" => Ge,
        other => bail!("unknown comparison operator `{other}`"),
    })
}

fn parse_bin_op(s: &str) -> Result<slater_scalar::BinOp> {
    use slater_scalar::BinOp::*;
    Ok(match s {
        "+" => Add,
        "-" => Sub,
        "*" => Mul,
        "/" => Div,
        "%" => Mod,
        other => bail!("unknown arithmetic operator `{other}`"),
    })
}

fn parse_node_overwrite(pair: Pair<Rule>) -> Result<Statement> {
    // node_overwrite = merge_node | match_set_node
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty node overwrite"))?;
    let is_merge = inner.as_rule() == Rule::merge_node;
    let mut node_pattern: Option<Pair<Rule>> = None;
    let mut set_props: Vec<(String, SetExpr)> = Vec::new();
    for c in inner.into_inner() {
        match c.as_rule() {
            Rule::node_pattern => node_pattern = Some(c),
            Rule::set_clause => set_props = parse_set_clause(c)?,
            _ => {}
        }
    }
    let np = node_pattern.ok_or_else(|| anyhow!("overwrite missing node pattern"))?;
    let match_ = node_match_from_pattern(np)?;
    Ok(Statement::NodeOverwrite(NodeOverwriteStmt {
        match_,
        is_merge,
        set_props,
    }))
}

fn parse_edge_overwrite(pair: Pair<Rule>) -> Result<Statement> {
    // The MERGE/MATCH keyword is an inline literal (not a captured rule), so detect
    // it from the leading token.
    let is_merge = pair
        .as_str()
        .get(..5)
        .map(|s| s.eq_ignore_ascii_case("merge"))
        .unwrap_or(false);
    let mut rel: Option<Pair<Rule>> = None;
    let mut set_props: Vec<(String, Value)> = Vec::new();
    for c in pair.into_inner() {
        match c.as_rule() {
            Rule::overwrite_rel => rel = Some(c),
            // Edges store only literal SET values; functions / prop refs are a
            // node-only feature, rejected here.
            Rule::set_clause => set_props = parse_set_clause_literal(c)?,
            _ => {}
        }
    }
    let rel = rel.ok_or_else(|| anyhow!("edge overwrite missing relationship pattern"))?;

    // overwrite_rel = node_pattern "-[" var? rel_detail "]->" node_pattern
    let mut patterns: Vec<Pair<Rule>> = Vec::new();
    let mut reltype = String::new();
    for c in rel.into_inner() {
        match c.as_rule() {
            Rule::node_pattern => patterns.push(c),
            Rule::rel_detail => {
                for d in c.into_inner() {
                    if d.as_rule() == Rule::reltype {
                        reltype = unquote_key(d.as_str());
                    }
                    // A property map on the matched relationship is ignored: edges are
                    // located by (src, dst, reltype), not by rel-property in v1.
                }
            }
            _ => {}
        }
    }
    if patterns.len() != 2 {
        bail!("edge overwrite must match exactly two endpoints");
    }
    if reltype.is_empty() {
        bail!("edge overwrite missing relationship type");
    }
    let src = node_match_from_pattern(patterns.remove(0))?;
    let dst = node_match_from_pattern(patterns.remove(0))?;
    Ok(Statement::EdgeOverwrite(EdgeOverwriteStmt {
        src,
        dst,
        reltype,
        is_merge,
        set_props,
    }))
}

fn parse_create_index(pair: Pair<Rule>) -> Result<Statement> {
    let mut entity = Entity::Node;
    let mut label_or_type = String::new();
    let mut property = String::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::node_index_target => {
                entity = Entity::Node;
                // (var? ":" label)
                for c in child.into_inner() {
                    if c.as_rule() == Rule::label {
                        label_or_type = unquote_key(c.as_str());
                    }
                }
            }
            Rule::edge_index_target => {
                entity = Entity::Edge;
                for c in child.into_inner() {
                    if c.as_rule() == Rule::reltype {
                        label_or_type = unquote_key(c.as_str());
                    }
                }
            }
            Rule::index_prop => {
                // index_prop = var "." key
                for c in child.into_inner() {
                    if c.as_rule() == Rule::key {
                        property = unquote_key(c.as_str());
                    }
                }
            }
            _ => {}
        }
    }
    Ok(Statement::RangeIndex(RangeIndexStmt {
        entity,
        label_or_type,
        property,
    }))
}

enum ArgOrder {
    /// `(label, prop, dim, metric)`
    Call,
    /// `(label, dim, metric, property)`
    Helper,
}

fn parse_vector_index(pair: Pair<Rule>, order: ArgOrder) -> Result<Statement> {
    let arg_list = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::arg_list)
        .ok_or_else(|| anyhow!("vector index call missing arguments"))?;
    let args: Vec<Value> = arg_list
        .into_inner()
        .filter(|p| p.as_rule() == Rule::value)
        .map(parse_value)
        .collect::<Result<_>>()?;

    let (label, property, dim, metric) = match order {
        ArgOrder::Call => {
            if args.len() < 4 {
                bail!("createNodeIndex expects (label, prop, dim, metric)");
            }
            (
                as_str(&args[0])?,
                as_str(&args[1])?,
                as_dim(&args[2])?,
                as_str(&args[3])?,
            )
        }
        ArgOrder::Helper => {
            if args.len() < 4 {
                bail!("createNodeVectorIndex expects (label, dim, metric, property)");
            }
            (
                as_str(&args[0])?,
                as_str(&args[3])?,
                as_dim(&args[1])?,
                as_str(&args[2])?,
            )
        }
    };
    Ok(Statement::VectorIndex(VectorIndexStmt {
        label,
        property,
        dim,
        metric,
    }))
}

// --- pattern / value helpers ---------------------------------------------------

/// `(variable?, labels, properties)` extracted from a `node_pattern`.
type ParsedPattern = (Option<String>, Vec<String>, Vec<(String, Value)>);

/// Parse a `node_pattern` into `(variable?, labels, props)`.
fn parse_node_pattern(pair: Pair<Rule>) -> Result<ParsedPattern> {
    let mut var = None;
    let mut labels = Vec::new();
    let mut props = Vec::new();
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::var => var = Some(child.as_str().to_string()),
            Rule::labels => {
                for l in child.into_inner() {
                    if l.as_rule() == Rule::label {
                        labels.push(unquote_key(l.as_str()));
                    }
                }
            }
            Rule::prop_map => props = parse_prop_map(child)?,
            _ => {}
        }
    }
    Ok((var, labels, props))
}

fn parse_prop_map(pair: Pair<Rule>) -> Result<Vec<(String, Value)>> {
    let mut out = Vec::new();
    for entry in pair.into_inner() {
        if entry.as_rule() != Rule::prop_entry {
            continue;
        }
        let mut key = String::new();
        let mut value = Value::Null;
        for c in entry.into_inner() {
            match c.as_rule() {
                Rule::key => key = unquote_key(c.as_str()),
                Rule::value => value = parse_value(c)?,
                _ => {}
            }
        }
        out.push((key, value));
    }
    Ok(out)
}

/// Parse a `value` pair (descends to its single concrete child).
fn parse_value(pair: Pair<Rule>) -> Result<Value> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty value"))?;
    match inner.as_rule() {
        Rule::vecf32 => {
            let mut xs = Vec::new();
            for nl in inner.into_inner() {
                if nl.as_rule() == Rule::num_list {
                    for n in nl.into_inner() {
                        xs.push(
                            n.as_str()
                                .parse::<f32>()
                                .context("parse vecf32 component")?,
                        );
                    }
                }
            }
            Ok(Value::Vector(xs))
        }
        Rule::list => {
            let mut items = Vec::new();
            for v in inner.into_inner() {
                if v.as_rule() == Rule::value {
                    items.push(parse_value(v)?);
                }
            }
            Ok(Value::List(items))
        }
        Rule::string => Ok(Value::Str(parse_string(inner))),
        Rule::boolean => Ok(Value::Bool(inner.as_str().eq_ignore_ascii_case("true"))),
        Rule::null_kw => Ok(Value::Null),
        Rule::number => {
            let s = inner.as_str();
            if s.contains('.') || s.contains('e') || s.contains('E') {
                Ok(Value::Float(s.parse::<f64>().context("parse float")?))
            } else {
                Ok(Value::Int(s.parse::<i64>().context("parse int")?))
            }
        }
        other => bail!("unexpected value rule {other:?}"),
    }
}

/// Decode a quoted `string` pair (single- or double-quoted) into its text,
/// resolving backslash escapes.
fn parse_string(pair: Pair<Rule>) -> String {
    let mut out = String::new();
    for inner in pair.into_inner() {
        // sq_inner / dq_inner — the raw inner text with escapes still present.
        let raw = inner.as_str();
        let mut chars = raw.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some('r') => out.push('\r'),
                    Some('\\') => out.push('\\'),
                    Some('\'') => out.push('\''),
                    Some('"') => out.push('"'),
                    Some('0') => out.push('\0'),
                    Some(other) => out.push(other),
                    None => {}
                }
            } else {
                out.push(c);
            }
        }
    }
    out
}

/// Decode an identifier token (label, reltype, property key) into its text: a
/// backtick-quoted name loses its outer backticks and un-doubles the inner ones
/// (`` `a``b` `` → ``a`b``, `` `` `` → the empty name); a bare name is itself. The
/// inverse of the emitters' `quote_ident` (`slater::consolidate`), and the reason a
/// hostile property key cannot splice structure into a rebuilt dump (HIK-84).
fn unquote_key(s: &str) -> String {
    match s.strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
        Some(inner) => inner.replace("``", "`"),
        None => s.to_string(),
    }
}

fn as_int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
        _ => None,
    }
}

fn as_str(v: &Value) -> Result<String> {
    match v {
        Value::Str(s) => Ok(s.clone()),
        other => bail!("expected a string argument, got {}", other.type_name()),
    }
}

fn as_dim(v: &Value) -> Result<u32> {
    as_int(v)
        .filter(|i| *i > 0)
        .map(|i| i as u32)
        .ok_or_else(|| anyhow!("expected a positive integer dimension"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Entity;
    use std::io::BufReader;

    fn stmts(script: &str) -> Vec<String> {
        let mut r = StatementReader::new(BufReader::new(script.as_bytes()));
        let mut out = Vec::new();
        while let Some(s) = r.next_statement().unwrap() {
            out.push(s);
        }
        out
    }

    #[test]
    fn non_utf8_input_is_rejected_not_silently_corrupted() {
        // A statement carrying an invalid UTF-8 byte (0xFF) must error, not be silently
        // repaired into U+FFFD (which `from_utf8_lossy` would have done).
        let mut bytes = b"CREATE (:A {t: '".to_vec();
        bytes.push(0xFF);
        bytes.extend_from_slice(b"'});");
        let mut r = StatementReader::new(BufReader::new(&bytes[..]));
        assert!(r.next_statement().is_err());
    }

    #[test]
    fn many_empty_statements_do_not_recurse() {
        // A long run of stray `;` used to recurse once per empty statement; make sure
        // it scans iteratively and simply skips them (no stack overflow, no phantom
        // statements). 100k separators is far past any tail-call safety net.
        let mut script = ";".repeat(100_000);
        script.push_str("CREATE (:A {});");
        let v = stmts(&script);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], "CREATE (:A {})");
    }

    #[test]
    fn splitter_respects_strings_and_spans_lines() {
        // A `;` and newlines inside a quoted markdown value must NOT split.
        let script = "CREATE (:A {t: 'line one;\nline two', n: 1});\nCREATE (:B {});";
        let v = stmts(script);
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("line one;"));
        assert!(v[0].contains("line two"));
        // Trailing statement without a final `;` is still emitted.
        let v2 = stmts("CREATE (:A {})");
        assert_eq!(v2.len(), 1);
    }

    /// HIK-84: a backtick-quoted identifier is opaque to the splitter — a `;` or a
    /// quote inside a hostile property key must not end the statement (that is exactly
    /// how an un-quoted key used to splice a second statement into the rebuild).
    #[test]
    fn splitter_treats_a_quoted_identifier_as_opaque() {
        let script = "MERGE (n:A {k: 1}) SET n.```x`` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a` = 1;\nCREATE (:B {});";
        let v = stmts(script);
        assert_eq!(
            v.len(),
            2,
            "a quoted name was split on its inner `;`: {v:?}"
        );
        assert!(v[0].starts_with("MERGE (n:A"));
        assert_eq!(v[1], "CREATE (:B {})");
        // A backtick *inside a string literal* is text, not an identifier quote — the
        // string state machine still wins.
        let v2 = stmts("CREATE (:A {t: 'a ` b'});\nCREATE (:B {});");
        assert_eq!(
            v2.len(),
            2,
            "a backtick in a string desynced the splitter: {v2:?}"
        );
    }

    /// HIK-84: the parser accepts every form the emitters (`slater::consolidate` /
    /// `slater::dump`) now produce, and decodes it back to the original name — labels,
    /// reltypes, keys and index properties, including the doubled-backtick escape.
    #[test]
    fn quoted_identifiers_round_trip_through_the_parser() {
        let hostile_key = "`x` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a";
        let s = "MERGE (n:`Odd Label`:`Zz) CREATE (:Pwned {x:1}) //` {`id key`: 'k1'}) \
                 SET n.```x`` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a` = 1";
        let Statement::NodeOverwrite(n) = parse_statement(s).unwrap() else {
            panic!("expected a node overwrite");
        };
        assert_eq!(n.match_.label, "Odd Label");
        assert_eq!(n.match_.extra_labels, vec!["Zz) CREATE (:Pwned {x:1}) //"]);
        assert_eq!(n.match_.key, "id key");
        assert_eq!(n.match_.value, Value::Str("k1".into()));
        // One SET assignment — the payload is a *name*, not a spliced statement.
        assert_eq!(n.set_props.len(), 1);
        assert_eq!(n.set_props[0].0, hostile_key);

        // Reltype + edge endpoints.
        let e = "MERGE (a:`Odd Label` {`id key`: 'k1'})-[r:`KNOWS OF; //`]->(b:`Odd Label` {`id key`: 'k2'})";
        let Statement::EdgeOverwrite(e) = parse_statement(e).unwrap() else {
            panic!("expected an edge overwrite");
        };
        assert_eq!(e.reltype, "KNOWS OF; //");
        assert_eq!(e.src.label, "Odd Label");
        assert_eq!(e.dst.key, "id key");

        // Index DDL, both entity forms.
        let Statement::RangeIndex(ni) =
            parse_statement("CREATE INDEX FOR (n:`Odd Label`) ON (n.`id key`)").unwrap()
        else {
            panic!("expected a range index");
        };
        assert_eq!(ni.entity, Entity::Node);
        assert_eq!(ni.label_or_type, "Odd Label");
        assert_eq!(ni.property, "id key");
        let Statement::RangeIndex(ei) =
            parse_statement("CREATE INDEX FOR ()-[r:`KNOWS OF; //`]->() ON (r.`w t`)").unwrap()
        else {
            panic!("expected a range index");
        };
        assert_eq!(ei.entity, Entity::Edge);
        assert_eq!(ei.label_or_type, "KNOWS OF; //");
        assert_eq!(ei.property, "w t");

        // A bare name is unchanged, and the empty name has a spelling.
        assert_eq!(unquote_key("Person"), "Person");
        assert_eq!(unquote_key("``"), "");
        assert_eq!(unquote_key("`a``b`"), "a`b");
    }

    #[test]
    fn accepts_node_create_with_all_value_kinds() {
        let s = r#"CREATE (:Chunk:__DumpVertex__ {__dump_id__: 7, title: 'A \'quoted\' name', body: "multi\nline", score: -0.5, big: 12, flag: true, missing: null, tags: ['a', 'b'], embedding: vecf32([0.1, -0.2, 3.0e-1])})"#;
        let Statement::Node(n) = parse_statement(s).unwrap() else {
            panic!("expected node");
        };
        assert_eq!(n.labels, vec!["Chunk", "__DumpVertex__"]);
        let get = |k: &str| {
            n.props
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("__dump_id__"), Some(Value::Int(7)));
        assert_eq!(get("title"), Some(Value::Str("A 'quoted' name".into())));
        assert_eq!(get("body"), Some(Value::Str("multi\nline".into())));
        assert_eq!(get("score"), Some(Value::Float(-0.5)));
        assert_eq!(get("big"), Some(Value::Int(12)));
        assert_eq!(get("flag"), Some(Value::Bool(true)));
        assert_eq!(get("missing"), Some(Value::Null));
        assert_eq!(
            get("tags"),
            Some(Value::List(vec![
                Value::Str("a".into()),
                Value::Str("b".into())
            ]))
        );
        assert_eq!(get("embedding"), Some(Value::Vector(vec![0.1, -0.2, 0.3])));
    }

    #[test]
    fn accepts_edge_create_and_resolves_endpoints() {
        let s = "MATCH (a:__DumpVertex__ {__dump_id__: 3}), (b:__DumpVertex__ {__dump_id__: 9}) CREATE (a)-[:CITES {weight: 2}]->(b)";
        let Statement::Edge(e) = parse_statement(s).unwrap() else {
            panic!("expected edge");
        };
        assert_eq!(e.src_dump_id, 3);
        assert_eq!(e.dst_dump_id, 9);
        assert_eq!(e.reltype, "CITES");
        assert_eq!(e.props, vec![("weight".to_string(), Value::Int(2))]);
    }

    #[test]
    fn edge_endpoint_order_follows_create_vars() {
        // CREATE writes (b)-> (a): src must resolve to b's dump_id, not the
        // MATCH order.
        let s = "MATCH (a:X {__dump_id__: 1}), (b:X {__dump_id__: 2}) CREATE (b)-[:R]->(a)";
        let Statement::Edge(e) = parse_statement(s).unwrap() else {
            panic!("expected edge");
        };
        assert_eq!(e.src_dump_id, 2);
        assert_eq!(e.dst_dump_id, 1);
    }

    #[test]
    fn accepts_node_merge_with_case_and_operators() {
        // The BioAperture "affiliation append" idiom: CASE + `=` + `+` in a node SET.
        let s = "MERGE (p:Person {id: 'x'}) SET p.affiliationSummary = \
                 CASE WHEN coalesce(p.affiliationSummary, '') = '' THEN 'Acme' \
                 ELSE p.affiliationSummary + '; ' + 'Acme' END";
        let Statement::NodeOverwrite(n) = parse_statement(s).unwrap() else {
            panic!("expected node overwrite");
        };
        assert!(n.is_merge);
        assert_eq!(n.set_props.len(), 1);
        let (key, expr) = &n.set_props[0];
        assert_eq!(key, "affiliationSummary");
        let SetExpr::Case {
            subject,
            whens,
            els,
        } = expr
        else {
            panic!("expected CASE, got {expr:?}");
        };
        assert!(subject.is_none(), "searched CASE has no subject");
        assert_eq!(whens.len(), 1);
        let (cond, then) = &whens[0];
        // condition is a `=` comparison; THEN is the bare literal.
        assert!(matches!(
            cond,
            SetExpr::Cmp {
                op: slater_scalar::CmpOp::Eq,
                ..
            }
        ));
        assert_eq!(*then, SetExpr::Lit(Value::Str("Acme".into())));
        // ELSE is a `+` concatenation chain.
        assert!(matches!(
            els.as_deref(),
            Some(SetExpr::BinOp {
                op: slater_scalar::BinOp::Add,
                ..
            })
        ));
    }

    #[test]
    fn accepts_node_and_edge_range_indexes() {
        let Statement::RangeIndex(n) =
            parse_statement("CREATE INDEX FOR (n:Provision) ON (n.celex)").unwrap()
        else {
            panic!("node index");
        };
        assert_eq!(n.entity, Entity::Node);
        assert_eq!(n.label_or_type, "Provision");
        assert_eq!(n.property, "celex");

        let Statement::RangeIndex(e) =
            parse_statement("CREATE INDEX FOR ()-[r:CITES]->() ON (r.weight)").unwrap()
        else {
            panic!("edge index");
        };
        assert_eq!(e.entity, Entity::Edge);
        assert_eq!(e.label_or_type, "CITES");
        assert_eq!(e.property, "weight");
    }

    #[test]
    fn accepts_both_vector_index_forms() {
        let Statement::VectorIndex(c) = parse_statement(
            "CALL db.idx.vector.createNodeIndex('Chunk', 'embedding', 1024, 'cosine')",
        )
        .unwrap() else {
            panic!("call form");
        };
        assert_eq!(
            (
                c.label.as_str(),
                c.property.as_str(),
                c.dim,
                c.metric.as_str()
            ),
            ("Chunk", "embedding", 1024, "cosine")
        );

        let Statement::VectorIndex(h) =
            parse_statement("createNodeVectorIndex('Chunk', 1024, 'cosine', 'embedding')").unwrap()
        else {
            panic!("helper form");
        };
        assert_eq!(
            (
                h.label.as_str(),
                h.property.as_str(),
                h.dim,
                h.metric.as_str()
            ),
            ("Chunk", "embedding", 1024, "cosine")
        );
    }

    #[test]
    fn marker_and_cleanup_lines_parse() {
        // The marker index is a structural range index (builder drops it later).
        assert!(matches!(
            parse_statement("CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__)").unwrap(),
            Statement::RangeIndex(_)
        ));
        // Cleanup + drop are Ignored.
        assert_eq!(
            parse_statement("MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__")
                .unwrap(),
            Statement::Ignored
        );
        assert_eq!(
            parse_statement("DROP INDEX ON :__DumpVertex__(__dump_id__)").unwrap(),
            Statement::Ignored
        );
    }

    #[test]
    fn rejects_malformed_and_unsupported() {
        // Unbalanced parens.
        assert!(parse_statement("CREATE (:L {a: 1}").is_err());
        // Truncated relationship.
        assert!(parse_statement("MATCH (a:X {__dump_id__: 1}) CREATE (a)-[:R]->").is_err());
        // Not a dump statement at all.
        assert!(parse_statement("SELECT * FROM whatever").is_err());
        // A bare MATCH with no SET / CREATE / REMOVE is still not a statement.
        assert!(parse_statement("MATCH (n:L {id: 1})").is_err());
        // An overwrite match pattern needs exactly one label and one match property.
        assert!(parse_statement("MATCH (n {id: 1}) SET n.a = 2").is_err());
        assert!(parse_statement("MATCH (n:L) SET n.a = 2").is_err());
        assert!(parse_statement("MATCH (n:L {a: 1, b: 2}) SET n.x = 3").is_err());
        // SET with no assignment.
        assert!(parse_statement("MATCH (n:L {id: 1}) SET").is_err());
        // Malformed vecf32.
        assert!(parse_statement("CREATE (:L {e: vecf32([1.0, 2.0)})").is_err());
    }

    #[test]
    fn accepts_node_overwrite_merge_and_match() {
        // MERGE with a SET clause.
        let Statement::NodeOverwrite(m) =
            parse_statement("MERGE (n:Concept {name: 'A'}) SET n.score = 99, n.note = 'x'")
                .unwrap()
        else {
            panic!("expected node overwrite");
        };
        assert!(m.is_merge);
        assert_eq!(m.match_.label, "Concept");
        assert_eq!(m.match_.key, "name");
        assert_eq!(m.match_.value, Value::Str("A".into()));
        assert_eq!(
            m.set_props,
            vec![
                ("score".to_string(), SetExpr::Lit(Value::Int(99))),
                ("note".to_string(), SetExpr::Lit(Value::Str("x".into()))),
            ]
        );

        // MATCH … SET is an overwrite, not is_merge.
        let Statement::NodeOverwrite(m) =
            parse_statement("MATCH (n:Concept {name: 'A'}) SET n.score = 1").unwrap()
        else {
            panic!("expected node overwrite");
        };
        assert!(!m.is_merge);
        assert_eq!(
            m.set_props,
            vec![("score".to_string(), SetExpr::Lit(Value::Int(1)))]
        );

        // MERGE with no SET (ensure-exists) parses with empty set_props.
        let Statement::NodeOverwrite(m) = parse_statement("MERGE (n:Concept {name: 'A'})").unwrap()
        else {
            panic!("expected node overwrite");
        };
        assert!(m.is_merge);
        assert!(m.set_props.is_empty());
    }

    #[test]
    fn parses_coalesce_and_function_set_rhs() {
        let Statement::NodeOverwrite(m) = parse_statement(
            "MERGE (n:Company {ticker: 'IMC'}) SET n.name = coalesce(n.name, n.canonicalName, 'X (Y), Z')",
        )
        .unwrap() else {
            panic!("expected node overwrite");
        };
        assert_eq!(
            m.set_props,
            vec![(
                "name".to_string(),
                SetExpr::Func {
                    name: "coalesce".to_string(),
                    args: vec![
                        SetExpr::Prop("name".to_string()),
                        SetExpr::Prop("canonicalName".to_string()),
                        SetExpr::Lit(Value::Str("X (Y), Z".into())),
                    ],
                }
            )]
        );

        // Nested calls and a dotted function name parse.
        let Statement::NodeOverwrite(m) =
            parse_statement("MERGE (n:X {id: 'a'}) SET n.u = toUpper(n.name)").unwrap()
        else {
            panic!("expected node overwrite");
        };
        assert_eq!(
            m.set_props,
            vec![(
                "u".to_string(),
                SetExpr::Func {
                    name: "toUpper".to_string(),
                    args: vec![SetExpr::Prop("name".to_string())],
                }
            )]
        );

        // A function in an edge SET is rejected (literal-only context).
        assert!(parse_statement(
            "MERGE (a:X {id: 'a'})-[r:T]->(b:Y {id: 'b'}) SET r.w = toUpper('a')"
        )
        .is_err());
    }

    #[test]
    fn accepts_edge_overwrite() {
        let Statement::EdgeOverwrite(e) = parse_statement(
            "MATCH (a:Concept {name: 'A'})-[r:LINK]->(b:Concept {name: 'B'}) SET r.w = 7",
        )
        .unwrap() else {
            panic!("expected edge overwrite");
        };
        assert!(!e.is_merge);
        assert_eq!(e.reltype, "LINK");
        assert_eq!(
            (e.src.label.as_str(), e.src.key.as_str()),
            ("Concept", "name")
        );
        assert_eq!(e.src.value, Value::Str("A".into()));
        assert_eq!(e.dst.value, Value::Str("B".into()));
        assert_eq!(e.set_props, vec![("w".to_string(), Value::Int(7))]);

        // The MERGE form and a relationship without an `r` variable both parse.
        let Statement::EdgeOverwrite(e) =
            parse_statement("MERGE (a:L {k: 1})-[:T]->(b:M {j: 2}) SET r.x = 0").unwrap()
        else {
            panic!("expected edge overwrite");
        };
        assert!(e.is_merge);
        assert_eq!(e.reltype, "T");

        // The bare edge MERGE form (no SET) parses with empty set_props — a
        // property-less relationship, as emitted by business-key MERGE dumps.
        let Statement::EdgeOverwrite(e) = parse_statement(
            "MERGE (a:Person {id: '9e'})-[r:SOURCED_FROM]->(b:Source {sourceId: '0001'})",
        )
        .unwrap() else {
            panic!("expected edge overwrite");
        };
        assert!(e.is_merge);
        assert_eq!(e.reltype, "SOURCED_FROM");
        assert_eq!(e.src.value, Value::Str("9e".into()));
        assert_eq!(e.dst.value, Value::Str("0001".into()));
        assert!(e.set_props.is_empty(), "bare edge MERGE has no SET props");
    }
}
