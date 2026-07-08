// SPDX-License-Identifier: Apache-2.0
//! The `dump` CLI subcommand: export a graph from a **running** server to
//! `slater-build`-compatible business-key `MERGE` Cypher.
//!
//! ```text
//! slater dump [GRAPH] -u USER [--host H] [--port P] [-o FILE] \
//!             [--key Label=prop]… [--pk FIELD]
//! slater dump --list -u USER                      # graphs the user may read
//! ```
//!
//! Unlike `slater query` (which mounts a generation in-process), `dump` connects
//! over **Bolt**, authenticates, and honours per-graph ACLs — so it works against
//! a live deployment with no disk access. The password is read from stdin (or the
//! `SLATER_DUMP_PASSWORD` env var), never a flag, to keep it out of `ps`/shell
//! history. The emitted dialect is byte-for-byte the business-key `MERGE` import
//! that `slater-build` ingests, so a graph round-trips: dump → `slater-build` →
//! new generation.
//!
//! It runs synchronously before the server's tokio runtime is built (the Bolt
//! client is blocking stdlib), mirroring the `diagnostics`/`query` subcommands.

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use crate::bolt::client::BoltClient;
use crate::bolt::packstream::PsValue;

/// PackStream struct tags for a Bolt `Node` / `Relationship` value (Bolt 5.x).
const TAG_NODE: u8 = 0x4E;
const TAG_RELATIONSHIP: u8 = 0x52;

/// Parsed `dump` invocation.
#[derive(Debug, Parser)]
#[command(
    name = "slater dump",
    about = "Export a graph to business-key MERGE Cypher"
)]
struct DumpArgs {
    /// Graph to dump (required unless `--list`).
    graph: Option<String>,

    /// List the graphs the authenticated user may read, then exit.
    #[arg(short = 'l', long)]
    list: bool,

    /// Server host.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Server Bolt port (defaults to the configured `server.port`).
    #[arg(long)]
    port: Option<u16>,

    /// User to authenticate as.
    #[arg(short = 'u', long)]
    user: String,

    /// Read the password from stdin (the default); given for parity with Docker's
    /// convention. The password is always taken from `SLATER_DUMP_PASSWORD` if set,
    /// else read as one line from stdin.
    #[arg(long)]
    password_stdin: bool,

    /// Output file (default: stdout).
    #[arg(short = 'o', long)]
    out: Option<PathBuf>,

    /// Identity-key override for a label: `--key Label=prop` (repeatable). Overrides
    /// the range-indexed property inferred for that label.
    #[arg(long = "key", value_name = "Label=prop")]
    key: Vec<String>,

    /// Global identity key: a single field used as every label's business key
    /// (dump_id-style). Takes precedence over inferred keys; `--key` still overrides
    /// a specific label on top of it.
    #[arg(long)]
    pk: Option<String>,
}

/// Handle the `dump` CLI subcommand and exit if present.
///
/// No-op unless `argv[1] == "dump"`, so it can be called unconditionally near the
/// top of `main`. Always exits the process on the `dump` path: `0` on success,
/// `1` on any error (message on stderr). `default_port` is the configured
/// `server.port`, used when `--port` is omitted.
pub fn dump_subcommand(default_port: u16) {
    if std::env::args().nth(1).as_deref() != Some("dump") {
        return;
    }
    // clap treats the first item as the program name; feed it "dump" (argv[1]) as
    // the pseudo-bin and the flags after it, so `graph` is the first positional.
    let args = DumpArgs::parse_from(std::env::args().skip(1));
    match run(args, default_port) {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("slater dump failed: {e:#}");
            std::process::exit(1);
        }
    }
}

/// Read the password from `SLATER_DUMP_PASSWORD` if set, else one line from stdin
/// (trailing newline stripped). `force_stdin` (the `--password-stdin` toggle)
/// always reads stdin, ignoring the env var. Empty is allowed (an open ACL).
fn read_password(force_stdin: bool) -> Result<String> {
    if !force_stdin {
        if let Ok(p) = std::env::var("SLATER_DUMP_PASSWORD") {
            return Ok(p);
        }
    }
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading password from stdin")?;
    Ok(line.trim_end_matches(['\n', '\r']).to_string())
}

/// Connect, authenticate, and dispatch to `--list` or the graph dump.
fn run(args: DumpArgs, default_port: u16) -> Result<()> {
    if !args.list && args.graph.is_none() {
        bail!("no graph named — pass a GRAPH argument, or --list to see readable graphs");
    }
    let port = args.port.unwrap_or(default_port);
    let password = read_password(args.password_stdin)?;

    // A generous read timeout: a large graph's `PULL` streams many records, but
    // the server sends them continuously, so the timeout only fires on a genuine
    // stall or a dropped connection.
    let mut conn = BoltClient::connect(&args.host, port, Duration::from_secs(120))
        .with_context(|| format!("connecting to {}:{}", args.host, port))?;
    conn.login("slater-dump/1.0", &args.user, &password)
        .context("authenticating (check --user and the password)")?;

    if args.list {
        return list_graphs(&mut conn);
    }

    let graph = args.graph.as_deref().expect("graph presence checked above");
    let overrides = parse_key_overrides(&args.key)?;
    let schema = fetch_schema(&mut conn, graph, &overrides, args.pk.as_deref())
        .with_context(|| format!("reading the schema of graph '{graph}'"))?;

    // Buffer the whole dump, then write it out in one shot — so a mid-dump failure
    // (an unidentifiable node, a lost connection) never leaves a truncated file.
    let mut buf: Vec<u8> = Vec::new();
    let warnings = dump_graph(&mut conn, graph, &schema, &mut buf)
        .with_context(|| format!("dumping graph '{graph}'"))?;

    match args.out.as_deref() {
        Some(path) => std::fs::write(path, &buf)
            .with_context(|| format!("writing dump to {}", path.display()))?,
        None => {
            let stdout = std::io::stdout();
            let mut w = stdout.lock();
            w.write_all(&buf).context("writing dump to stdout")?;
            w.flush().ok();
        }
    }
    for warning in warnings {
        eprintln!("slater dump: warning: {warning}");
    }
    Ok(())
}

/// `--list`: print the graph names the authenticated user may read, one per line,
/// to stdout. Backed by the server's `SHOW DATABASES` (which already filters to
/// the caller's read grants via the ACL).
fn list_graphs(conn: &mut BoltClient) -> Result<()> {
    let (_cols, rows) = conn
        .run_pull("SHOW DATABASES", None)
        .context("listing graphs (SHOW DATABASES)")?;
    let stdout = std::io::stdout();
    let mut w = std::io::BufWriter::new(stdout.lock());
    for row in rows {
        if let Some(name) = row
            .first()
            .and_then(crate::bolt::packstream::PsValue::as_str)
        {
            writeln!(w, "{name}").context("writing graph name")?;
        }
    }
    w.flush().context("flushing stdout")?;
    Ok(())
}

// ── Schema + identity-key resolution ─────────────────────────────────────────

/// The resolved dump schema: which property is each label's business key, plus the
/// range indexes to re-create so a rebuild can resolve those keys.
struct Schema {
    /// Per-label identity-key overrides (`--key Label=prop`).
    overrides: BTreeMap<String, String>,
    /// Global identity key (`--pk`): the fallback key for every label.
    pk: Option<String>,
    /// Key inferred from a label's node range index.
    inferred: BTreeMap<String, String>,
    /// Node range indexes to emit as `CREATE INDEX FOR (n:L) ON (n.p)`.
    node_indexes: std::collections::BTreeSet<(String, String)>,
    /// Relationship range indexes to emit as `CREATE INDEX FOR ()-[r:T]->() ON (r.p)`.
    rel_indexes: std::collections::BTreeSet<(String, String)>,
}

impl Schema {
    /// The identity key for `label`, applying precedence `--key` > `--pk` >
    /// inferred-from-index. `None` when the label has no resolvable key.
    fn key_for(&self, label: &str) -> Option<&str> {
        if let Some(k) = self.overrides.get(label) {
            return Some(k);
        }
        if let Some(k) = &self.pk {
            return Some(k);
        }
        self.inferred.get(label).map(String::as_str)
    }
}

/// Parse `--key Label=prop` overrides into a map. A malformed entry (no `=`, empty
/// side) is a hard error.
fn parse_key_overrides(items: &[String]) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for item in items {
        let (label, prop) = item
            .split_once('=')
            .with_context(|| format!("--key must be `Label=prop`, got `{item}`"))?;
        if label.is_empty() || prop.is_empty() {
            bail!("--key must be `Label=prop` with both sides non-empty, got `{item}`");
        }
        map.insert(label.to_string(), prop.to_string());
    }
    Ok(map)
}

/// Look a column up by name in a `(columns, rows)` result and return the row's
/// value at that position.
fn col<'a>(columns: &[String], row: &'a [PsValue], name: &str) -> Option<&'a PsValue> {
    let idx = columns.iter().position(|c| c == name)?;
    row.get(idx)
}

/// The first string element of a `List` value (index/label columns are single-item
/// lists in Neo4j's `SHOW INDEXES`).
fn first_str(v: Option<&PsValue>) -> Option<String> {
    match v {
        Some(PsValue::List(items)) => items.first().and_then(PsValue::as_str).map(str::to_string),
        _ => None,
    }
}

/// Query the graph's labels and range indexes over Bolt and resolve identity keys.
fn fetch_schema(
    conn: &mut BoltClient,
    graph: &str,
    overrides: &BTreeMap<String, String>,
    pk: Option<&str>,
) -> Result<Schema> {
    // Range indexes → inferred identity key per node label + the DDL to re-create.
    let (icols, irows) = conn.run_pull("SHOW INDEXES", Some(graph))?;
    let mut inferred: BTreeMap<String, String> = BTreeMap::new();
    let mut node_indexes = std::collections::BTreeSet::new();
    let mut rel_indexes = std::collections::BTreeSet::new();
    for row in &irows {
        // Only RANGE indexes carry a business key we can seek on.
        let kind = col(&icols, row, "type")
            .and_then(PsValue::as_str)
            .unwrap_or("");
        if !kind.eq_ignore_ascii_case("RANGE") {
            continue;
        }
        let entity = col(&icols, row, "entityType")
            .and_then(PsValue::as_str)
            .unwrap_or("");
        let (Some(name), Some(prop)) = (
            first_str(col(&icols, row, "labelsOrTypes")),
            first_str(col(&icols, row, "properties")),
        ) else {
            continue;
        };
        if entity.eq_ignore_ascii_case("NODE") {
            // First index wins as the inferred key (deterministic: SHOW INDEXES is
            // manifest-ordered).
            inferred.entry(name.clone()).or_insert_with(|| prop.clone());
            node_indexes.insert((name, prop));
        } else if entity.eq_ignore_ascii_case("RELATIONSHIP") {
            rel_indexes.insert((name, prop));
        }
    }

    // Every label — so we can emit `CREATE INDEX` for identity keys up front and
    // detect labels with no resolvable key before scanning nodes.
    let (lcols, lrows) = conn.run_pull("CALL db.labels()", Some(graph))?;
    let schema = Schema {
        overrides: overrides.clone(),
        pk: pk.map(str::to_string),
        inferred,
        node_indexes,
        rel_indexes,
    };
    // Fold each label's resolved identity (label, key) into the node index set so a
    // `--key`/`--pk` key that is not itself range-indexed still gets a CREATE INDEX
    // (the rebuild needs it indexed to resolve the business key).
    let mut schema = schema;
    for row in &lrows {
        if let Some(label) = col(&lcols, row, "label")
            .or_else(|| row.first())
            .and_then(PsValue::as_str)
        {
            if let Some(key) = schema.key_for(label) {
                schema
                    .node_indexes
                    .insert((label.to_string(), key.to_string()));
            }
        }
    }
    Ok(schema)
}

// ── Graph dump ───────────────────────────────────────────────────────────────

/// Dump `graph`'s schema DDL, nodes and edges as business-key `MERGE` Cypher into
/// `out`. Returns any non-fatal warnings (e.g. properties dropped because they are
/// vectors/temporals a `MERGE` dump cannot carry). A node with no resolvable
/// identity key is a hard error.
fn dump_graph(
    conn: &mut BoltClient,
    graph: &str,
    schema: &Schema,
    out: &mut impl Write,
) -> Result<Vec<String>> {
    let mut warnings: Vec<String> = Vec::new();

    // No header comment: `slater-build` splits on `;` and has no comment syntax, so
    // any preamble would be parsed as (broken) Cypher. The dump is emitted as pure
    // rebuildable statements. CREATE INDEX DDL first, so the rebuild recreates
    // indexes before the MERGEs that rely on them. Sorted for a deterministic dump.
    for (label, prop) in &schema.node_indexes {
        writeln!(out, "CREATE INDEX FOR (n:{label}) ON (n.{prop});")?;
    }
    for (rtype, prop) in &schema.rel_indexes {
        writeln!(out, "CREATE INDEX FOR ()-[r:{rtype}]->() ON (r.{prop});")?;
    }

    // Nodes: one MERGE per node, keyed on its resolved business key.
    let (_ncols, nrows) = conn
        .run_pull("MATCH (n) RETURN n", Some(graph))
        .context("scanning nodes (MATCH (n) RETURN n)")?;
    for row in &nrows {
        let node = row.first().context("node row is empty")?;
        emit_node(node, schema, out, &mut warnings)?;
    }

    // Edges: one MERGE per relationship, keyed on both endpoints' business keys.
    let (_ecols, erows) = conn
        .run_pull("MATCH (a)-[r]->(b) RETURN a, r, b", Some(graph))
        .context("scanning edges (MATCH (a)-[r]->(b) RETURN a, r, b)")?;
    for row in &erows {
        let (Some(a), Some(r), Some(b)) = (row.first(), row.get(1), row.get(2)) else {
            bail!("edge row does not have three fields (a, r, b)");
        };
        emit_edge(a, r, b, schema, out, &mut warnings)?;
    }
    Ok(warnings)
}

/// A borrowed Bolt `Node`'s `(labels, properties)` — labels as PackStream values,
/// properties as name→value pairs (insertion order preserved).
type NodeParts<'a> = (&'a [PsValue], &'a [(String, PsValue)]);

/// Borrow a Bolt `Node` value's `(labels, properties)`.
fn node_parts(v: &PsValue) -> Option<NodeParts<'_>> {
    match v {
        PsValue::Struct { tag, fields } if *tag == TAG_NODE => {
            let labels = match fields.get(1) {
                Some(PsValue::List(items)) => items.as_slice(),
                _ => &[],
            };
            let props = match fields.get(2) {
                Some(PsValue::Map(entries)) => entries.as_slice(),
                _ => &[],
            };
            Some((labels, props))
        }
        _ => None,
    }
}

/// Resolve a node's `(identity label, key property, key value)` from its labels +
/// properties. Labels are tried in sorted order for a deterministic choice.
fn node_identity<'a>(
    v: &'a PsValue,
    schema: &'a Schema,
) -> Result<(&'a str, &'a str, &'a PsValue)> {
    let (labels, props) = node_parts(v).context("expected a Node value")?;
    let mut names: Vec<&'a str> = labels.iter().filter_map(PsValue::as_str).collect();
    names.sort_unstable();
    for &label in &names {
        // `key_for`'s output lifetime is tied to `&schema` (= 'a), so `key` and the
        // found value both outlive the loop and can be returned.
        let Some(key) = schema.key_for(label) else {
            continue;
        };
        if let Some((_, val)) = props.iter().find(|(k, _)| k == key) {
            return Ok((label, key, val));
        }
    }
    bail!(
        "node with labels {names:?} has no resolvable identity key — add `--key Label=prop` \
         for one of them, or `--pk <field>`"
    )
}

/// `MERGE (n:Label {key: v}) SET n.p = v, …;` for one node.
fn emit_node(
    v: &PsValue,
    schema: &Schema,
    out: &mut impl Write,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let (label, key, key_val) = node_identity(v, schema)?;
    let key_lit = literal(key_val).with_context(|| {
        format!("node identity {label}.{key} has a value that cannot be a MERGE key")
    })?;
    let (_labels, props) = node_parts(v).expect("node_parts succeeded in node_identity");
    write!(out, "MERGE (n:{label} {{{key}: {key_lit}}})")?;
    emit_set(
        props,
        "n",
        Some(key),
        out,
        warnings,
        &format!("{label} node"),
    )?;
    writeln!(out, ";")?;
    Ok(())
}

/// `MERGE (a:LA {ka: va})-[r:T]->(b:LB {kb: vb}) SET r.p = v, …;` for one edge.
fn emit_edge(
    a: &PsValue,
    r: &PsValue,
    b: &PsValue,
    schema: &Schema,
    out: &mut impl Write,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let (al, ak, av) = node_identity(a, schema)?;
    let (bl, bk, bv) = node_identity(b, schema)?;
    let (rtype, rprops) = match r {
        PsValue::Struct { tag, fields } if *tag == TAG_RELATIONSHIP => {
            let ty = fields.get(3).and_then(PsValue::as_str).unwrap_or_default();
            let props = match fields.get(4) {
                Some(PsValue::Map(entries)) => entries.as_slice(),
                _ => &[][..],
            };
            (ty, props)
        }
        _ => bail!("expected a Relationship value in the edge row"),
    };
    let (av_lit, bv_lit) = (
        literal(av).context("edge source identity value cannot be a MERGE key")?,
        literal(bv).context("edge target identity value cannot be a MERGE key")?,
    );
    write!(
        out,
        "MERGE (a:{al} {{{ak}: {av_lit}}})-[r:{rtype}]->(b:{bl} {{{bk}: {bv_lit}}})"
    )?;
    emit_set(rprops, "r", None, out, warnings, &format!("{rtype} edge"))?;
    writeln!(out, ";")?;
    Ok(())
}

/// Append ` SET <var>.<p> = <lit>, …` for every property except `exclude` (the
/// business key), sorted by name for determinism. A property whose value cannot be
/// a `MERGE`-dump literal (vector/temporal/map) is skipped with a warning.
fn emit_set(
    props: &[(String, PsValue)],
    var: &str,
    exclude: Option<&str>,
    out: &mut impl Write,
    warnings: &mut Vec<String>,
    context: &str,
) -> Result<()> {
    let mut kept: Vec<(&String, String)> = Vec::new();
    for (name, val) in props {
        if exclude == Some(name.as_str()) {
            continue;
        }
        match literal(val) {
            Some(lit) => kept.push((name, lit)),
            None => warnings.push(format!(
                "{context}: dropped property `{name}` (a vector/temporal/map value cannot ride a MERGE dump)"
            )),
        }
    }
    kept.sort_by(|a, b| a.0.cmp(b.0));
    for (i, (name, lit)) in kept.iter().enumerate() {
        let sep = if i == 0 { " SET" } else { "," };
        write!(out, "{sep} {var}.{name} = {lit}")?;
    }
    Ok(())
}

// ── Cypher-literal escaper (PsValue → slater-build dialect) ───────────────────

/// Render a Bolt scalar as a `slater-build`-dialect Cypher literal that round-trips
/// through the builder's parser. Returns `None` for a value with no `MERGE`-dump
/// spelling (vector/temporal struct, or a map) — mirrors `consolidate::literal`,
/// which operates on the internal `Value` type.
fn literal(v: &PsValue) -> Option<String> {
    match v {
        PsValue::Null => Some("null".to_string()),
        PsValue::Bool(b) => Some(b.to_string()),
        PsValue::Int(i) => Some(i.to_string()),
        PsValue::Float(f) => Some(format_float(*f)),
        PsValue::String(s) => Some(quote_str(s)),
        PsValue::List(items) => {
            let mut inner = Vec::with_capacity(items.len());
            for it in items {
                inner.push(literal(it)?);
            }
            Some(format!("[{}]", inner.join(", ")))
        }
        // Vectors/temporals arrive as structs; maps/bytes have no scalar spelling.
        PsValue::Map(_) | PsValue::Struct { .. } | PsValue::Bytes(_) => None,
    }
}

/// Format an `f64` so it re-parses as a float, never an int (`.0` suffix when there
/// is no `.`/`e`). Non-finite has no spelling → `null`. Mirrors `consolidate`.
fn format_float(f: f64) -> String {
    if !f.is_finite() {
        return "null".to_string();
    }
    let s = format!("{f}");
    if s.bytes().any(|b| b == b'.' || b == b'e' || b == b'E') {
        s
    } else {
        format!("{s}.0")
    }
}

/// Single-quote and escape a string, matching `slater-build`'s `parse_string`
/// unescaping (`\\`, `\'`, `\n`, `\t`, `\r`, `\0`). Mirrors `consolidate::quote_str`.
fn quote_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// clap's own consistency check on the derived CLI (catches duplicate flags,
    /// bad `short`/`long` combos, etc.).
    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        DumpArgs::command().debug_assert();
    }

    #[test]
    fn parses_a_graph_dump_invocation() {
        let a = DumpArgs::parse_from([
            "dump",
            "people",
            "-u",
            "alice",
            "--host",
            "db.internal",
            "--port",
            "7690",
        ]);
        assert_eq!(a.graph.as_deref(), Some("people"));
        assert_eq!(a.user, "alice");
        assert_eq!(a.host, "db.internal");
        assert_eq!(a.port, Some(7690));
        assert!(!a.list);
    }

    #[test]
    fn parses_list_without_a_graph_and_defaults_host() {
        let a = DumpArgs::parse_from(["dump", "--list", "-u", "bob"]);
        assert!(a.list);
        assert_eq!(a.graph, None);
        assert_eq!(a.host, "localhost");
        assert_eq!(a.port, None);
    }

    #[test]
    fn neither_graph_nor_list_is_rejected() {
        // clap parses (user only); `run` rejects the missing target before any I/O.
        let a = DumpArgs::parse_from(["dump", "-u", "bob"]);
        let err = run(a, 7687).unwrap_err();
        assert!(
            err.to_string().contains("no graph named"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn password_prefers_the_env_var_unless_forced_to_stdin() {
        // Only assert the env-var branch (the stdin branch would block on a tty in
        // the test harness); `force_stdin` bypassing the env var is covered by
        // construction — the env path is the one with observable behaviour here.
        std::env::set_var("SLATER_DUMP_PASSWORD", "s3cr3t");
        assert_eq!(read_password(false).unwrap(), "s3cr3t");
        std::env::remove_var("SLATER_DUMP_PASSWORD");
    }

    // ── Cypher-literal escaper (must match `consolidate::literal` exactly) ──

    #[test]
    fn literal_matches_the_builder_dialect() {
        assert_eq!(literal(&PsValue::Null).unwrap(), "null");
        assert_eq!(literal(&PsValue::Bool(true)).unwrap(), "true");
        assert_eq!(literal(&PsValue::Int(-7)).unwrap(), "-7");
        assert_eq!(literal(&PsValue::Float(2.5)).unwrap(), "2.5");
        // A whole-valued float keeps a decimal point so it re-parses as a float.
        assert_eq!(literal(&PsValue::Float(10.0)).unwrap(), "10.0");
        assert_eq!(format_float(f64::NAN), "null");
        assert_eq!(literal(&PsValue::str("plain")).unwrap(), "'plain'");
        // Escapes match the builder's `parse_string` unescaping.
        assert_eq!(
            literal(&PsValue::str("a'b\\c\nd")).unwrap(),
            "'a\\'b\\\\c\\nd'"
        );
        assert_eq!(
            literal(&PsValue::List(vec![PsValue::Int(1), PsValue::str("x")])).unwrap(),
            "[1, 'x']"
        );
        // Vectors/temporals (structs), maps, and bytes have no MERGE-dump spelling.
        assert!(literal(&PsValue::Struct {
            tag: 0x58,
            fields: vec![]
        })
        .is_none());
        assert!(literal(&PsValue::Map(vec![])).is_none());
        assert!(literal(&PsValue::Bytes(vec![1, 2])).is_none());
    }

    // ── Identity-key resolution ──

    #[test]
    fn parse_key_overrides_ok_and_rejects_malformed() {
        let ok = parse_key_overrides(&["Person=name".into(), "Org=id".into()]).unwrap();
        assert_eq!(ok.get("Person").unwrap(), "name");
        assert_eq!(ok.get("Org").unwrap(), "id");
        assert!(parse_key_overrides(&["noequals".into()]).is_err());
        assert!(parse_key_overrides(&["=empty".into()]).is_err());
        assert!(parse_key_overrides(&["Label=".into()]).is_err());
    }

    fn schema(pk: Option<&str>, over: &[(&str, &str)], infer: &[(&str, &str)]) -> Schema {
        Schema {
            overrides: over
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            pk: pk.map(str::to_string),
            inferred: infer
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            node_indexes: Default::default(),
            rel_indexes: Default::default(),
        }
    }

    #[test]
    fn key_for_precedence_is_override_then_pk_then_inferred() {
        // override beats pk beats inferred.
        let s = schema(Some("gid"), &[("Person", "email")], &[("Person", "name")]);
        assert_eq!(s.key_for("Person"), Some("email"));
        // no override → pk wins over inferred.
        let s = schema(Some("gid"), &[], &[("Person", "name")]);
        assert_eq!(s.key_for("Person"), Some("gid"));
        // no override, no pk → inferred.
        let s = schema(None, &[], &[("Person", "name")]);
        assert_eq!(s.key_for("Person"), Some("name"));
        // nothing resolvable.
        let s = schema(None, &[], &[]);
        assert_eq!(s.key_for("Ghost"), None);
    }

    // ── Node/edge emission over synthetic Bolt values ──

    fn node(id: i64, labels: &[&str], props: &[(&str, PsValue)]) -> PsValue {
        PsValue::Struct {
            tag: TAG_NODE,
            fields: vec![
                PsValue::Int(id),
                PsValue::List(labels.iter().map(|l| PsValue::str(*l)).collect()),
                PsValue::Map(
                    props
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                ),
                PsValue::str(id.to_string()),
            ],
        }
    }

    fn rel(ty: &str, props: &[(&str, PsValue)]) -> PsValue {
        PsValue::Struct {
            tag: TAG_RELATIONSHIP,
            fields: vec![
                PsValue::Int(1),
                PsValue::Int(0),
                PsValue::Int(0),
                PsValue::str(ty),
                PsValue::Map(
                    props
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                ),
            ],
        }
    }

    fn render(
        f: impl FnOnce(&mut Vec<u8>, &mut Vec<String>) -> Result<()>,
    ) -> (String, Vec<String>) {
        let mut buf = Vec::new();
        let mut warns = Vec::new();
        f(&mut buf, &mut warns).unwrap();
        (String::from_utf8(buf).unwrap(), warns)
    }

    #[test]
    fn emit_node_renders_business_key_merge_excluding_the_key() {
        let s = schema(None, &[], &[("Person", "name")]);
        let n = node(
            0,
            &["Person"],
            &[("name", PsValue::str("Alice")), ("age", PsValue::Int(30))],
        );
        let (out, warns) = render(|o, w| emit_node(&n, &s, o, w));
        assert_eq!(out, "MERGE (n:Person {name: 'Alice'}) SET n.age = 30;\n");
        assert!(warns.is_empty());
    }

    #[test]
    fn emit_node_picks_the_sorted_label_with_a_key() {
        // Two labels; only `Person` has a key, and labels are tried sorted so the
        // choice is deterministic regardless of Bolt's label order.
        let s = schema(None, &[], &[("Person", "name")]);
        let n = node(0, &["Zebra", "Person"], &[("name", PsValue::str("Bob"))]);
        let (out, _) = render(|o, w| emit_node(&n, &s, o, w));
        assert_eq!(out, "MERGE (n:Person {name: 'Bob'});\n");
    }

    #[test]
    fn emit_node_errors_when_no_label_has_a_key() {
        let s = schema(None, &[], &[]);
        let n = node(0, &["Widget"], &[("sku", PsValue::str("X"))]);
        let mut buf = Vec::new();
        let mut warns = Vec::new();
        let err = emit_node(&n, &s, &mut buf, &mut warns).unwrap_err();
        assert!(
            err.to_string().contains("no resolvable identity key"),
            "{err}"
        );
    }

    #[test]
    fn emit_node_drops_a_vector_property_with_a_warning() {
        let s = schema(None, &[], &[("Doc", "id")]);
        // A vector prop arrives as a struct; it cannot ride the MERGE dump.
        let embedding = PsValue::Struct {
            tag: 0x56,
            fields: vec![],
        };
        let n = node(
            0,
            &["Doc"],
            &[("id", PsValue::str("d1")), ("embedding", embedding)],
        );
        let (out, warns) = render(|o, w| emit_node(&n, &s, o, w));
        assert_eq!(out, "MERGE (n:Doc {id: 'd1'});\n");
        assert_eq!(warns.len(), 1);
        assert!(warns[0].contains("embedding"), "{:?}", warns);
    }

    #[test]
    fn emit_edge_renders_both_endpoint_keys_and_rel_props() {
        let s = schema(None, &[], &[("Person", "name")]);
        let a = node(0, &["Person"], &[("name", PsValue::str("Alice"))]);
        let b = node(1, &["Person"], &[("name", PsValue::str("Bob"))]);
        let r = rel("KNOWS", &[("since", PsValue::Int(2020))]);
        let (out, _) = render(|o, w| emit_edge(&a, &r, &b, &s, o, w));
        assert_eq!(
            out,
            "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;\n"
        );
    }

    #[test]
    fn emit_set_sorts_properties_by_name() {
        let s = schema(None, &[], &[("Person", "name")]);
        let n = node(
            0,
            &["Person"],
            &[
                ("name", PsValue::str("A")),
                ("zeta", PsValue::Int(1)),
                ("alpha", PsValue::Int(2)),
            ],
        );
        let (out, _) = render(|o, w| emit_node(&n, &s, o, w));
        // key excluded; remaining props alphabetical.
        assert_eq!(
            out,
            "MERGE (n:Person {name: 'A'}) SET n.alpha = 2, n.zeta = 1;\n"
        );
    }
}
