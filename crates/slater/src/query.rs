// SPDX-License-Identifier: Apache-2.0
//! The `query` CLI subcommand: a one-shot, in-process Cypher run.
//!
//! Mounts one graph's `current` generation (honoring the configured storage
//! backend, at-rest encryption key, and copy-completeness check), parses and
//! executes a single read-only Cypher query against it, prints the result as a
//! JSON object on stdout, and exits — no Bolt listener, no socket.
//!
//! ```text
//! slater query [GRAPH] [-q|--quiet] [-n|--repeat N] '<CYPHER>'
//! ```
//!
//! `-n`/`--repeat N` executes the query N times against the warming block cache
//! (default 1) — a quick cold→warm latency probe. Each run logs its own metrics
//! summary; only the final run's result is printed.
//!
//! `GRAPH` defaults to `defaultGraph` from config when omitted. `-q`/`--quiet`
//! suppresses house logging entirely (no tracing subscriber is installed), so
//! the only thing on stdout is the compact result JSON — safe to pipe into `jq`
//! or capture in a script. Without `--quiet`, normal `hs_utils` logging is
//! initialised at the configured level; its datestamped lines (config, "opened
//! generation", …) are written to stdout — the house default — alongside the
//! pretty-printed result JSON, matching the operator-facing behaviour of the
//! other subcommands. Either way the result JSON itself is results only.
//!
//! After a successful run a one-line `query executed` summary is logged with the
//! query `cost` (elements charged), `resultCount`, `execMs`, and — only when the
//! query carries a literal `LIMIT` — `limitRowCount`. It is metrics-only: it
//! never echoes the query text or any result value, and `-q` suppresses it along
//! with all other logging.
//!
//! The query path runs synchronously, before the server's tokio runtime is
//! built (the S3 backend bridges its own async on a private runtime), mirroring
//! the `diagnostics`/`healthcheck` subcommands.

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value as Json};
use std::time::{Duration, Instant};
use tracing::info;

use crate::cache::BlockCache;
use crate::config::AppConfig;
use crate::exec::{Engine, QueryResult, Val};
use crate::generation::Generation;
use crate::{parser, server};

/// Handle the `query` CLI subcommand and exit if present.
///
/// No-op unless `argv[1] == "query"`, so it can be called unconditionally near
/// the top of `main` (after config load — it needs the resolved `cfg`). On the
/// `query` path it always exits the process: `0` on success, `1` on any error
/// (with the message on stderr). `cfg` is borrowed, not consumed, so the normal
/// server path is untouched when no `query` arg is present.
pub fn query_subcommand(cfg: &AppConfig) {
    if std::env::args().nth(1).as_deref() != Some("query") {
        return;
    }
    match run(cfg) {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("slater query failed: {e:#}");
            std::process::exit(1);
        }
    }
}

/// Parsed `query` invocation: the (optional) graph, the Cypher text, the quiet
/// flag, and how many times to execute the query.
struct Args {
    graph: Option<String>,
    cypher: String,
    quiet: bool,
    repeat: u32,
}

/// Parse `argv[2..]` into [`Args`]. Flags may appear anywhere: `-q`/`--quiet`,
/// and `-n`/`--repeat N` (also `--repeat=N`) to run the query N times (≥1,
/// default 1) against the warm block cache. The remaining positionals are
/// `[GRAPH] <CYPHER>` (one positional ⇒ just the query, two ⇒ graph then query).
fn parse_args() -> Result<Args> {
    let mut quiet = false;
    let mut repeat: u32 = 1;
    let mut positionals: Vec<String> = Vec::new();
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "-q" | "--quiet" => quiet = true,
            "-n" | "--repeat" => {
                let v = it
                    .next()
                    .context("-n/--repeat requires a count (e.g. -n 3)")?;
                repeat = parse_repeat(&v)?;
            }
            s if s.starts_with("--repeat=") => repeat = parse_repeat(&s["--repeat=".len()..])?,
            _ => positionals.push(arg),
        }
    }
    let (graph, cypher) = match positionals.len() {
        1 => (None, positionals.pop().unwrap()),
        2 => {
            let cypher = positionals.pop().unwrap();
            (positionals.pop(), cypher)
        }
        _ => bail!(
            "usage: slater query [GRAPH] [-q|--quiet] [-n|--repeat N] '<CYPHER>'\n\
             provide the Cypher query (and optionally the graph name) as positional arguments"
        ),
    };
    if cypher.trim().is_empty() {
        bail!("the Cypher query is empty");
    }
    Ok(Args {
        graph,
        cypher,
        quiet,
        repeat,
    })
}

/// Parse a `--repeat` count: a positive integer.
fn parse_repeat(s: &str) -> Result<u32> {
    let n: u32 = s
        .parse()
        .with_context(|| format!("invalid --repeat count {s:?} (expected a positive integer)"))?;
    if n == 0 {
        bail!("--repeat count must be at least 1");
    }
    Ok(n)
}

/// Mount the generation, run the query, and print the JSON result.
fn run(cfg: &AppConfig) -> Result<()> {
    let args = parse_args()?;

    // `-q` suppresses house logging: with no tracing subscriber installed, every
    // `info!` the open/exec path emits is dropped and stdout carries only the
    // result JSON. Without `-q` we initialise normal `hs_utils` logging at the
    // configured level, so its datestamped lines (including "opened generation")
    // are written to stdout — the house default — alongside the result. The JSON
    // result is results only in both cases.
    if !args.quiet {
        hs_utils::logging::init(&cfg.log.level);
    }

    let graph = match &args.graph {
        Some(g) => g.clone(),
        None => {
            if cfg.default_graph.is_empty() {
                bail!(
                    "no graph given and defaultGraph is unset — pass the graph name: \
                     slater query <GRAPH> '<CYPHER>'"
                );
            }
            cfg.default_graph.clone()
        }
    };

    // Mount: the configured backend (fs/S3 + optional disk cache), the at-rest
    // master key, and the copy-completeness verification policy — exactly as the
    // server opens generations.
    cfg.encryption
        .check_key_file_outside_data_dir(cfg.data_dir())
        .context("validate at-rest encryption key location")?;
    let store = server::build_store(cfg)?;
    let master_key = cfg
        .encryption
        .load_key()
        .context("load at-rest master key")?;
    let gen = Generation::open_with_store_opts(
        store.as_ref(),
        &graph,
        master_key.as_deref(),
        cfg.data_backend.verify_integrity_resolved(),
    )
    .with_context(|| format!("open generation for graph {graph:?}"))?;

    // Parse, then execute `repeat` times against the shared (warming) block
    // cache, applying the same per-query budgets the server does — so a one-shot
    // run behaves identically to the same query over Bolt. Only the final run's
    // result is rendered; each run logs its own metrics summary, so a repeated
    // run shows cold→warm execMs without printing the result N times.
    let ast = parser::parse(&args.cypher).context("parse query")?;
    let cache = BlockCache::new(cfg.cache.block_cache_bytes);
    let limit = query_limit(&ast);
    let mut result = None;
    for run in 1..=args.repeat {
        let mut engine = Engine::new(&gen, &cache)
            .with_max_rows(cfg.query.max_rows as usize)
            .with_max_intermediate(cfg.query.max_intermediate)
            .with_max_scan(cfg.query.max_scan)
            .with_max_shortest_path_explore(cfg.query.max_shortest_path_explore);
        if cfg.query.timeout_ms > 0 {
            engine =
                engine.with_deadline(Instant::now() + Duration::from_millis(cfg.query.timeout_ms));
        }
        let exec_start = Instant::now();
        let r = engine
            .run(&ast)
            .with_context(|| format!("execute query (run {run}/{})", args.repeat))?;
        let exec_ms = exec_start.elapsed().as_millis();

        // Execution summary on the log channel (suppressed under `-q`, since no
        // subscriber is installed). Metrics only — never the query text or any
        // result value: the per-query cost (elements charged), the row count,
        // the execution time, the `run` index when repeating, and the LIMIT row
        // cap when the query specified one.
        log_summary(
            args.repeat,
            run,
            engine.cost(),
            r.rows.len(),
            exec_ms,
            limit,
        );
        result = Some(r);
    }
    let result = result.expect("repeat is >= 1");

    // A fresh, limit-free engine resolves Node/Relationship records for output,
    // reusing the already-warm block cache.
    let render_engine = Engine::new(&gen, &cache);
    let out = result_to_json(&render_engine, &result)?;
    let rendered = if args.quiet {
        serde_json::to_string(&out).context("serialise result")?
    } else {
        serde_json::to_string_pretty(&out).context("serialise result")?
    };
    println!("{rendered}");
    Ok(())
}

/// The integer `LIMIT` row cap of the (head) query, if it specified one as a
/// literal. `None` when there is no `LIMIT`, or the limit is a `$param`/
/// expression we cannot resolve to a constant here — in which case the summary
/// log omits `limitRowCount` entirely.
fn query_limit(ast: &parser::ast::Query) -> Option<i64> {
    match ast.head.ret.body.limit.as_ref()? {
        parser::ast::Expr::Literal(graph_format::ids::Value::Int(n)) => Some(*n),
        _ => None,
    }
}

/// Emit the `query executed` metrics summary for one run. The `run` index is
/// included only when repeating (`repeat > 1`); `limitRowCount` only when the
/// query carried a literal `LIMIT`. Static field sets force the explicit arms.
fn log_summary(repeat: u32, run: u32, cost: u64, rows: usize, exec_ms: u128, limit: Option<i64>) {
    match (repeat > 1, limit) {
        (false, Some(l)) => {
            info!(
                cost,
                resultCount = rows,
                execMs = exec_ms,
                limitRowCount = l,
                "query executed"
            )
        }
        (false, None) => info!(cost, resultCount = rows, execMs = exec_ms, "query executed"),
        (true, Some(l)) => info!(
            run,
            cost,
            resultCount = rows,
            execMs = exec_ms,
            limitRowCount = l,
            "query executed"
        ),
        (true, None) => {
            info!(
                run,
                cost,
                resultCount = rows,
                execMs = exec_ms,
                "query executed"
            )
        }
    }
}

/// Render a [`QueryResult`] as `{"columns": [...], "rows": [[...], ...]}`.
fn result_to_json(engine: &Engine, result: &QueryResult) -> Result<Json> {
    let rows = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| val_to_json(engine, v))
                .collect::<Result<Vec<_>>>()
                .map(Json::Array)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({ "columns": result.columns.clone(), "rows": rows }))
}

/// Convert a runtime [`Val`] to a JSON value. Mirrors the Bolt `encode_val`
/// shapes (see `server.rs`) but targets JSON: nodes/relationships expand to
/// their labels/type + properties, temporals render as their ISO strings (JSON
/// has no temporal type), and a point becomes a `{latitude, longitude}` object.
fn val_to_json(engine: &Engine, v: &Val) -> Result<Json> {
    Ok(match v {
        Val::Null => Json::Null,
        Val::Bool(b) => Json::Bool(*b),
        Val::Int(i) => Json::from(*i),
        Val::Float(f) => Json::from(*f),
        Val::Str(s) => Json::String(s.clone()),
        Val::List(xs) => Json::Array(
            xs.iter()
                .map(|x| val_to_json(engine, x))
                .collect::<Result<_>>()?,
        ),
        // JSON has no vector type; a stored embedding renders as an array of floats.
        Val::Vector(xs) => Json::Array(xs.iter().map(|f| Json::from(*f as f64)).collect()),
        Val::Map(m) => Json::Object(pairs_to_json(engine, m)?),
        Val::Node(id) => {
            let (labels, props) = engine.node_record(*id)?;
            let mut obj = Map::new();
            obj.insert("id".into(), Json::from(*id));
            obj.insert(
                "labels".into(),
                Json::Array(labels.into_iter().map(Json::String).collect()),
            );
            obj.insert(
                "properties".into(),
                Json::Object(pairs_to_json(engine, &props)?),
            );
            Json::Object(obj)
        }
        Val::Rel {
            id,
            start,
            end,
            reltype,
        } => {
            let (type_name, props) = engine.rel_record(*id, *reltype)?;
            let mut obj = Map::new();
            obj.insert("id".into(), Json::from(*id));
            obj.insert("type".into(), Json::String(type_name));
            obj.insert("start".into(), Json::from(*start));
            obj.insert("end".into(), Json::from(*end));
            obj.insert(
                "properties".into(),
                Json::Object(pairs_to_json(engine, &props)?),
            );
            Json::Object(obj)
        }
        Val::Path { nodes, rels } => {
            let nodes = nodes
                .iter()
                .map(|id| val_to_json(engine, &Val::Node(*id)))
                .collect::<Result<Vec<_>>>()?;
            let rels = rels
                .iter()
                .map(|r| val_to_json(engine, r))
                .collect::<Result<Vec<_>>>()?;
            json!({ "nodes": nodes, "rels": rels })
        }
        Val::Point {
            latitude,
            longitude,
        } => json!({ "latitude": latitude, "longitude": longitude }),
        Val::Date(secs) => Json::String(crate::temporal::date_to_string(*secs)),
        Val::Time(secs) => Json::String(crate::temporal::time_to_string(*secs)),
        Val::DateTime(secs) => Json::String(crate::temporal::datetime_to_string(*secs)),
        Val::Duration(secs) => Json::String(crate::temporal::duration_to_string(*secs)),
    })
}

/// Convert a list of named [`Val`]s (node/rel properties, or a map) to a JSON
/// object, preserving key order.
fn pairs_to_json(engine: &Engine, pairs: &[(String, Val)]) -> Result<Map<String, Json>> {
    let mut obj = Map::new();
    for (k, v) in pairs {
        obj.insert(k.clone(), val_to_json(engine, v)?);
    }
    Ok(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testgen;

    /// Open the shared fixture and render `q` as the subcommand would.
    fn run_json(tag: &str, q: &str) -> Json {
        let (root, graph, _) = testgen::write_basic(tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let ast = parser::parse(q).unwrap();
        let result = Engine::new(&gen, &cache).run(&ast).unwrap();
        let render = Engine::new(&gen, &cache);
        result_to_json(&render, &result).unwrap()
    }

    #[test]
    fn scalar_result_shape() {
        let out = run_json("query_scalar", "MATCH (n:Person) RETURN count(n) AS c");
        assert_eq!(out["columns"], json!(["c"]));
        // Fixture has three :Person nodes (Alice, Bob, Carol).
        assert_eq!(out["rows"], json!([[3]]));
    }

    #[test]
    fn node_expands_to_labels_and_properties() {
        let out = run_json(
            "query_node",
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
        );
        assert_eq!(out["columns"], json!(["n"]));
        let node = &out["rows"][0][0];
        assert_eq!(node["labels"], json!(["Person"]));
        assert_eq!(node["properties"]["name"], json!("Alice"));
        assert_eq!(node["properties"]["age"], json!(30));
        assert_eq!(node["properties"]["city"], json!("London"));
        assert!(node["id"].is_number());
    }

    #[test]
    fn relationship_expands_to_type_and_endpoints() {
        let out = run_json(
            "query_rel",
            "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) RETURN r",
        );
        let rel = &out["rows"][0][0];
        assert_eq!(rel["type"], json!("KNOWS"));
        assert_eq!(rel["properties"]["since"], json!(2020));
        assert!(rel["start"].is_number() && rel["end"].is_number());
    }
}
