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

use crate::model::{EdgeStmt, Entity, NodeStmt, RangeIndexStmt, Statement, VectorIndexStmt};

#[derive(Parser)]
#[grammar = "primitive_cypher.pest"]
struct PrimitiveCypher;

// ---------------------------------------------------------------------------
// Streaming statement reader
// ---------------------------------------------------------------------------

/// Splits a dump script into statements on top-level `;`. Operates on bytes: the
/// delimiters it tracks (`;`, `'`, `"`, `\`) are all ASCII, and UTF-8
/// continuation bytes are always `>= 0x80`, so byte-level scanning never splits a
/// multibyte character or mistakes one for a delimiter.
///
// DESIGN: dump scripts can be very large (multi-paragraph markdown text fields),
// so we never slurp the whole file — we pull bytes from a `BufRead` and emit one
// statement at a time. A `;` is a separator only outside a string literal; inside
// a literal (single- or double-quoted, with `\` escapes) it is ordinary text.
pub struct StatementReader<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    in_string: Option<u8>,
    escaped: bool,
    done: bool,
}

impl<R: BufRead> StatementReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            in_string: None,
            escaped: false,
            done: false,
        }
    }

    /// Return the next non-empty statement (trimmed), or `None` at end of input.
    pub fn next_statement(&mut self) -> Result<Option<String>> {
        loop {
            // Drain whatever the BufReader already has, byte by byte, until a
            // top-level `;` completes a statement.
            let available = self.reader.fill_buf().context("read dump script")?;
            if available.is_empty() {
                // EOF: emit any trailing statement without a terminating `;`.
                if self.done {
                    return Ok(None);
                }
                self.done = true;
                return Ok(self.take_statement());
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
                } else {
                    match b {
                        b'\'' | b'"' => {
                            self.in_string = Some(b);
                            self.buf.push(b);
                        }
                        b';' => {
                            // Statement boundary.
                            self.reader.consume(consumed);
                            if let Some(s) = self.take_statement() {
                                return Ok(Some(s));
                            }
                            // Empty statement (e.g. stray `;`): keep scanning.
                            return self.next_statement();
                        }
                        _ => self.buf.push(b),
                    }
                }
            }
            self.reader.consume(consumed);
        }
    }

    fn take_statement(&mut self) -> Option<String> {
        let bytes = std::mem::take(&mut self.buf);
        let s = String::from_utf8_lossy(&bytes);
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Statement parsing
// ---------------------------------------------------------------------------

/// Parse one statement string (no trailing `;`) into a typed [`Statement`].
pub fn parse_statement(input: &str) -> Result<Statement> {
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
        Rule::edge_create => parse_edge_create(form),
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

fn parse_edge_create(pair: Pair<Rule>) -> Result<Statement> {
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
                    .find(|(k, _)| k == "__dump_id__")
                    .and_then(|(_, v)| as_int(v))
                    .ok_or_else(|| anyhow!("edge MATCH endpoint missing integer __dump_id__"))?;
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
                        Rule::reltype => reltype = d.as_str().to_string(),
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
                        label_or_type = c.as_str().to_string();
                    }
                }
            }
            Rule::edge_index_target => {
                entity = Entity::Edge;
                for c in child.into_inner() {
                    if c.as_rule() == Rule::reltype {
                        label_or_type = c.as_str().to_string();
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
                        labels.push(l.as_str().to_string());
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

fn unquote_key(s: &str) -> String {
    s.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(s)
        .to_string()
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
        // MERGE is not part of the dump dialect.
        assert!(parse_statement("MERGE (a:L {id: 1})").is_err());
        // Malformed vecf32.
        assert!(parse_statement("CREATE (:L {e: vecf32([1.0, 2.0)})").is_err());
    }
}
