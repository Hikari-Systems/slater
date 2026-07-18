// SPDX-License-Identifier: Apache-2.0
//! Query execution (run_query) and Bolt result encoding.
//!
//! Split out of `server.rs` as a child module (a pure relocation). Shared types,
//! consts and helpers stay in the parent, reached via `use super::*`; the parent
//! re-exports this module's items so sibling modules can call them by name.

use super::*;

pub(crate) async fn run_query(
    ctx: &Arc<ConnCtx>,
    gen: Arc<Generation>,
    query: &str,
    ast: parser::ast::Query,
    params: HashMap<String, Val>,
    version: (u8, u8),
    overlay: ReadOverlay,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let ReadOverlay {
        delta,
        epoch: delta_epoch,
        journal: rw_journal,
    } = overlay;
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
    let rw_indexes = ctx.rw_indexes.clone();
    let rw_cfg = ctx.rw_index_cfg;
    let result_cache = ctx.result_cache.clone();
    let key =
        ResultKey::with_delta_epoch(gen.uuid(), delta_epoch, result_query_key(query, &params));
    // Queries calling `rand()`/`randomUUID()`/`timestamp()` must re-run every
    // time, so they bypass the result cache (both lookup and store).
    let cacheable = !parser::is_nondeterministic(&ast);
    let max_rows = ctx.max_rows;
    let timeout_ms = ctx.timeout_ms;
    let max_intermediate = ctx.max_intermediate;
    let max_scan = ctx.max_scan;
    let intermediate_budget = ctx.intermediate_budget.clone();
    let max_shortest_path_explore = ctx.max_shortest_path_explore;
    let adj_stream_threshold = ctx.adj_stream_threshold;
    let adj_stream_chunk = ctx.adj_stream_chunk;
    let fanout_pool = ctx.fanout_pool.clone();
    let beam_width = ctx.beam_width;
    let temp_beam_width = ctx.temp_beam_width;
    let graph_name = gen.graph().to_string();
    // Gate all per-query instrumentation on the info level being active OR
    // load-test diagnostics being enabled: when both are off, we take no
    // timestamps and no cache snapshots, and build no QueryTiming — the hot path
    // is exactly what it was before instrumentation. The default log level is
    // `info`, so every query emits its `query executed` summary out of the box
    // (without the chatty `debug` SDK/wire tracing); raising the level to `warn`
    // restores the zero-overhead hot path. Diagnostics needs the same `total_ms`
    // for its latency histogram, so it shares this gate.
    let instrument = tracing::enabled!(Level::INFO) || ctx.diag.enabled;

    ctx.diag.on_query_start();
    let join =
        tokio::task::spawn_blocking(move || -> Result<(EncodedRows, Option<QueryTiming>)> {
            // Per-query instrumentation (only when `instrument`): wall-clock split into
            // execute vs encode, and the block-cache hit/miss/eviction delta this query
            // caused (the counters are process-wide, so we snapshot before/after). A
            // result-cache hit skips execution, which shows up as exec_ms ≈ 0.
            let t_start = instrument.then(Instant::now);
            let blk_before = instrument.then(|| cache.metrics());

            // Result-cache lookup (skipped for non-deterministic queries), then
            // execute-and-cache on a miss.
            let cached = if cacheable {
                result_cache.get(&key)
            } else {
                None
            };
            // `cost` is the elements charged against the query budget; it is only
            // meaningful when the query actually executed, so a result-cache hit
            // (no engine) reports `None` and the summary omits the field.
            // Overlay the writable-layer delta on the pinned core for this query's
            // whole life (`MergedView`). The empty-delta fast path makes the
            // read-only case behaviourally identical to reading the bare core.
            let view = MergedView::new(gen.as_ref(), delta);
            let (result, result_cache_hit, cost) = match cached {
                Some(r) => (r, true, None),
                None => {
                    let mut engine = Engine::new(&view, cache.as_ref())
                        .with_vector_cache(vector_cache.as_ref(), beam_width)
                        .with_temp_beam_width(temp_beam_width)
                        .with_params(params)
                        .with_max_rows(max_rows)
                        .with_max_intermediate(max_intermediate)
                        .with_max_scan(max_scan)
                        .with_global_budget(intermediate_budget.as_ref())
                        .with_max_shortest_path_explore(max_shortest_path_explore)
                        .with_adj_stream(adj_stream_threshold, adj_stream_chunk)
                        .with_fanout_pool(fanout_pool.clone());
                    // The RW-index arm of the delta's KNN. The epoch is the one taken with the
                    // snapshot above, in the same atomic read — the index is cut at exactly it.
                    if let Some(journal) = rw_journal {
                        engine =
                            engine.with_rw_index(rw_indexes.as_ref(), journal, delta_epoch, rw_cfg);
                    }
                    if timeout_ms > 0 {
                        engine = engine
                            .with_deadline(Instant::now() + Duration::from_millis(timeout_ms));
                    }
                    let r = Arc::new(engine.run(&ast)?);
                    let cost = engine.cost();
                    if cacheable {
                        let bytes = estimate_result_bytes(&r);
                        result_cache.insert(key.clone(), r.clone(), bytes);
                    }
                    (r, false, Some(cost))
                }
            };
            let t_after_exec = instrument.then(Instant::now);

            // Encode for this connection's version. A plain engine (no params/limits
            // needed) resolves Node/Relationship records through the shared block
            // cache — over the same merged view, so a returned node carries its
            // overlaid (patched) properties.
            let engine = Engine::new(&view, cache.as_ref());
            let mut rows = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                let mut encoded = Vec::with_capacity(row.len());
                for v in row {
                    encoded.push(encode_val(&engine, version, v)?);
                }
                rows.push(encoded);
            }

            let timing = if instrument {
                let t_end = Instant::now();
                let blk_after = cache.metrics();
                let blk_before = blk_before.unwrap();
                let t_start = t_start.unwrap();
                let t_after_exec = t_after_exec.unwrap();
                Some(QueryTiming {
                    result_cache_hit,
                    cost,
                    exec_ms: (t_after_exec - t_start).as_secs_f64() * 1e3,
                    encode_ms: (t_end - t_after_exec).as_secs_f64() * 1e3,
                    total_ms: (t_end - t_start).as_secs_f64() * 1e3,
                    rows: rows.len(),
                    blk_hits: blk_after.hits.saturating_sub(blk_before.hits),
                    blk_misses: blk_after.misses.saturating_sub(blk_before.misses),
                    blk_evictions: blk_after.evictions.saturating_sub(blk_before.evictions),
                })
            } else {
                None
            };
            Ok(((result.columns.clone(), rows), timing))
        })
        .await;

    match join {
        Ok(Ok((out, timing))) => {
            // Only ever `Some` when the info level was active (see `instrument`).
            // A block-cache miss is a cold block read (pread + decompress); many
            // misses on a small query is the signature of an unindexed scan. A high
            // total_ms with result_cache=miss and many blk_misses points at exactly
            // that.
            // Feed the diagnostics latency histogram (no-op when disabled). When
            // diagnostics are on, `instrument` is true so `timing` is always Some.
            let total_ms = timing.as_ref().map(|t| t.total_ms);
            if let Some(t) = timing {
                info!(
                    graph = %graph_name,
                    // A result-cache hit ran no engine, so it charges no budget:
                    // `cost = 0` alongside `result_cache = "hit"`.
                    cost = t.cost.unwrap_or(0),
                    rows = t.rows,
                    result_cache = if t.result_cache_hit { "hit" } else { "miss" },
                    exec_ms = format_args!("{:.1}", t.exec_ms),
                    encode_ms = format_args!("{:.1}", t.encode_ms),
                    total_ms = format_args!("{:.1}", t.total_ms),
                    blk_hits = t.blk_hits,
                    blk_misses = t.blk_misses,
                    blk_hit_ratio = format_args!("{:.2}", hit_ratio(t.blk_hits, t.blk_misses)),
                    blk_evicted = t.blk_evictions,
                    query = %log_query(query),
                    "query executed"
                );
            }
            ctx.diag.on_query_ok(total_ms.unwrap_or(0.0));
            Ok(out)
        }
        Ok(Err(e)) => {
            // A failed query emits no `query executed` summary (that only fires on
            // success), so without this line a budget trip / timeout is invisible in
            // the logs. Log at warn with the graph, reason and (truncated) query so
            // the next such failure is diagnosable.
            warn!(
                graph = %graph_name,
                error = %format!("{e:#}"),
                query = %log_query(query),
                "query failed"
            );
            ctx.diag.on_query_err(&e);
            Err(Failure::from_query_error(&e))
        }
        Err(e) => {
            warn!(
                graph = %graph_name,
                error = %e,
                query = %log_query(query),
                "query task failed"
            );
            ctx.diag.on_query_task_failed();
            Err(Failure::new(
                CODE_EXECUTION,
                format!("query task failed: {e}"),
            ))
        }
    }
}

/// Column names plus the PackStream-encoded rows — the shape `run_query`'s
/// blocking task produces.
type EncodedRows = (Vec<String>, Vec<Vec<PsValue>>);

/// Per-query timing + cache-delta, captured inside the blocking task and logged
/// once the result returns (see [`run_query`]).
struct QueryTiming {
    result_cache_hit: bool,
    /// Elements charged against the query budget (`Engine::cost`); `None` on a
    /// result-cache hit, where no engine ran.
    cost: Option<u64>,
    exec_ms: f64,
    encode_ms: f64,
    total_ms: f64,
    rows: usize,
    blk_hits: u64,
    blk_misses: u64,
    blk_evictions: u64,
}

/// Block-cache hit ratio `hits / (hits + misses)` for a single query, as a
/// fraction in `[0.0, 1.0]`. A query that touched no blocks (e.g. a pure
/// `RETURN 1`, or a result-cache hit) has no accesses and reports `0.0`.
pub(crate) fn hit_ratio(hits: u64, misses: u64) -> f64 {
    let total = hits + misses;
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

/// Collapse a query's whitespace and truncate it for a single-line log field.
pub(crate) fn log_query(query: &str) -> String {
    let one_line = query.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.chars().count() > 160 {
        let truncated: String = one_line.chars().take(160).collect();
        format!("{truncated}…")
    } else {
        one_line
    }
}

/// Build the normalised query portion of a [`ResultKey`]: the query text with
/// runs of whitespace collapsed, followed by the parameters serialised in a
/// deterministic (name-sorted) order. Two textually-different-but-equivalent
/// whitespace variants share a cache entry; differing params do not.
pub(crate) fn result_query_key(query: &str, params: &HashMap<String, Val>) -> String {
    let mut s = query.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut names: Vec<&String> = params.keys().collect();
    names.sort();
    for name in names {
        // \u{1} is not valid in a query, so it cannot collide with query content.
        s.push('\u{1}');
        s.push_str(name);
        s.push('=');
        s.push_str(&format!("{:?}", params[name]));
    }
    s
}

/// A coarse estimate of a result's resident footprint, used to charge it against
/// the result-cache budget. Exactness is not required — it only needs to grow with
/// the result so the byte budget bounds memory.
pub(crate) fn estimate_result_bytes(r: &QueryResult) -> usize {
    let cols: usize = r.columns.iter().map(|c| c.len() + 16).sum();
    let rows: usize = r
        .rows
        .iter()
        .map(|row| row.iter().map(val_bytes).sum::<usize>())
        .sum();
    cols + rows + 64
}

pub(crate) fn val_bytes(v: &Val) -> usize {
    match v {
        Val::Null | Val::Bool(_) | Val::Int(_) | Val::Float(_) => 16,
        Val::Str(s) => s.len() + 16,
        Val::List(xs) => 16 + xs.iter().map(val_bytes).sum::<usize>(),
        Val::Vector(xs) => 16 + xs.len() * 4,
        Val::Map(m) => {
            16 + m
                .iter()
                .map(|(k, x)| k.len() + 16 + val_bytes(x))
                .sum::<usize>()
        }
        Val::Node(_) => 24,
        Val::Rel { .. } => 40,
        Val::Path { nodes, rels } => {
            16 + nodes.len() * 24 + rels.iter().map(val_bytes).sum::<usize>()
        }
        Val::Point { .. } => 32,
        Val::Date(_) | Val::Time(_) | Val::DateTime(_) | Val::Duration(_) => 24,
    }
}

// ── Value encoding (exec::Val → PackStream) ───────────────────────────────────

/// Encode a runtime [`Val`] as a Bolt [`PsValue`]. `Node`/`Relationship` are
/// resolved against the engine (labels, type, properties); element-id fields are
/// emitted only for Bolt ≥ 5 (`version.0 >= 5`), matching the drivers' decoders.
pub(crate) fn encode_val<V: ReadView>(
    engine: &Engine<'_, V>,
    version: (u8, u8),
    v: &Val,
) -> Result<PsValue> {
    Ok(match v {
        Val::Null => PsValue::Null,
        Val::Bool(b) => PsValue::Bool(*b),
        Val::Int(i) => PsValue::Int(*i),
        Val::Float(f) => PsValue::Float(*f),
        Val::Str(s) => PsValue::String(s.clone()),
        Val::List(xs) => PsValue::List(
            xs.iter()
                .map(|x| encode_val(engine, version, x))
                .collect::<Result<_>>()?,
        ),
        // Bolt has no native vector type; a stored embedding returns as a list of floats.
        Val::Vector(xs) => PsValue::List(xs.iter().map(|f| PsValue::Float(*f as f64)).collect()),
        Val::Map(m) => PsValue::Map(encode_pairs(engine, version, m)?),
        Val::Node(id) => {
            let (labels, props) = engine.node_record(*id)?;
            let mut fields = vec![
                PsValue::Int(*id as i64),
                PsValue::List(labels.into_iter().map(PsValue::String).collect()),
                PsValue::Map(encode_pairs(engine, version, &props)?),
            ];
            if version.0 >= 5 {
                fields.push(PsValue::String(id.to_string())); // element_id
            }
            PsValue::Struct {
                tag: TAG_NODE,
                fields,
            }
        }
        Val::Rel {
            id,
            start,
            end,
            reltype,
        } => {
            let (type_name, props) = engine.rel_record(*id, *reltype)?;
            let mut fields = vec![
                PsValue::Int(*id as i64),
                PsValue::Int(*start as i64),
                PsValue::Int(*end as i64),
                PsValue::String(type_name),
                PsValue::Map(encode_pairs(engine, version, &props)?),
            ];
            if version.0 >= 5 {
                fields.push(PsValue::String(id.to_string())); // element_id
                fields.push(PsValue::String(start.to_string())); // start element_id
                fields.push(PsValue::String(end.to_string())); // end element_id
            }
            PsValue::Struct {
                tag: TAG_RELATIONSHIP,
                fields,
            }
        }
        // Bolt `Path` (0x50): a list of the distinct nodes (start first), a list of
        // the distinct relationships as `UnboundRelationship` (0x72) structures, and
        // an `indices` list weaving them into walk order. Each segment contributes a
        // pair `[rel_index, node_index]`: `rel_index` is 1-based into the rel list,
        // signed by traversal direction (+ when the edge's stored src→dst matches the
        // walk, − when reversed); `node_index` is 0-based into the node list of the
        // node reached. The walk starts at node 0. Validated against the Neo4j driver
        // decoder semantics, not FalkorDB's RESP path.
        Val::Path { nodes, rels } => {
            // Distinct nodes, preserving first-appearance order (start at index 0).
            let mut node_ids: Vec<u64> = Vec::new();
            let mut node_pos: HashMap<u64, usize> = HashMap::new();
            for &nid in nodes {
                node_pos.entry(nid).or_insert_with(|| {
                    node_ids.push(nid);
                    node_ids.len() - 1
                });
            }
            // Distinct relationships by id (a bidirectional walk may reuse an edge).
            let mut rel_pos: HashMap<u64, usize> = HashMap::new();
            let mut rel_order: Vec<&Val> = Vec::new();
            for r in rels {
                if let Val::Rel { id, .. } = r {
                    rel_pos.entry(*id).or_insert_with(|| {
                        rel_order.push(r);
                        rel_order.len() - 1
                    });
                }
            }
            let node_structs = node_ids
                .iter()
                .map(|id| encode_val(engine, version, &Val::Node(*id)))
                .collect::<Result<Vec<_>>>()?;
            let rel_structs = rel_order
                .iter()
                .map(|r| encode_unbound_rel(engine, version, r))
                .collect::<Result<Vec<_>>>()?;
            let mut indices = Vec::with_capacity(rels.len() * 2);
            for (k, r) in rels.iter().enumerate() {
                if let Val::Rel { id, start, end, .. } = r {
                    let from = nodes[k];
                    let to = nodes[k + 1];
                    let idx = (rel_pos[id] + 1) as i64;
                    let signed = if *start == from && *end == to {
                        idx
                    } else {
                        -idx
                    };
                    indices.push(PsValue::Int(signed));
                    indices.push(PsValue::Int(node_pos[&to] as i64));
                }
            }
            PsValue::Struct {
                tag: TAG_PATH,
                fields: vec![
                    PsValue::List(node_structs),
                    PsValue::List(rel_structs),
                    PsValue::List(indices),
                ],
            }
        }
        // Bolt `Point2D` (0x58): `[srid, x, y]`. FalkorDB always uses WGS-84, so
        // srid = 4326, x = longitude, y = latitude (resultset_replybolt.c). Not
        // yet byte-validated against a live Neo4j driver in this env (none
        // available); follows the published Point2D spec.
        Val::Point {
            latitude,
            longitude,
        } => PsValue::Struct {
            tag: TAG_POINT2D,
            fields: vec![
                PsValue::Int(4326),
                PsValue::Float(*longitude),
                PsValue::Float(*latitude),
            ],
        },
        // Bolt v2 temporals. Whole-second storage ⇒ `nanoseconds` is always 0.
        // Not byte-validated against a live driver here (same caveat as Path /
        // Point2D); follows the published Neo4j PackStream spec.
        Val::Date(secs) => PsValue::Struct {
            tag: TAG_DATE,
            fields: vec![PsValue::Int(secs.div_euclid(86_400))],
        },
        Val::Time(secs) => PsValue::Struct {
            tag: TAG_LOCAL_TIME,
            fields: vec![PsValue::Int(secs.rem_euclid(86_400) * 1_000_000_000)],
        },
        Val::DateTime(secs) => PsValue::Struct {
            tag: TAG_LOCAL_DATETIME,
            fields: vec![PsValue::Int(*secs), PsValue::Int(0)],
        },
        Val::Duration(secs) => {
            let d = crate::temporal::duration_components(*secs);
            PsValue::Struct {
                tag: TAG_DURATION,
                fields: vec![
                    PsValue::Int(d.years * 12 + d.months),
                    PsValue::Int(d.days),
                    PsValue::Int(d.hours * 3_600 + d.minutes * 60 + d.seconds),
                    PsValue::Int(0),
                ],
            }
        }
    })
}

/// Encode a `Val::Rel` as a Bolt `UnboundRelationship` (0x72): `[id, type, props]`
/// (plus the element-id field for Bolt ≥ 5). Endpoints are omitted — a path's node
/// list supplies them.
pub(crate) fn encode_unbound_rel<V: ReadView>(
    engine: &Engine<'_, V>,
    version: (u8, u8),
    r: &Val,
) -> Result<PsValue> {
    let Val::Rel { id, reltype, .. } = r else {
        bail!("encode_unbound_rel expects a relationship value");
    };
    let (type_name, props) = engine.rel_record(*id, *reltype)?;
    let mut fields = vec![
        PsValue::Int(*id as i64),
        PsValue::String(type_name),
        PsValue::Map(encode_pairs(engine, version, &props)?),
    ];
    if version.0 >= 5 {
        fields.push(PsValue::String(id.to_string())); // element_id
    }
    Ok(PsValue::Struct {
        tag: TAG_UNBOUND_REL,
        fields,
    })
}

pub(crate) fn encode_pairs<V: ReadView>(
    engine: &Engine<'_, V>,
    version: (u8, u8),
    pairs: &[(String, Val)],
) -> Result<Vec<(String, PsValue)>> {
    pairs
        .iter()
        .map(|(k, v)| Ok((k.clone(), encode_val(engine, version, v)?)))
        .collect()
}

/// Map Bolt `RUN` parameters (a PackStream map) into executor [`Val`]s.
pub(crate) fn params_to_vals(
    params: &PsValue,
) -> std::result::Result<HashMap<String, Val>, Failure> {
    let mut out = HashMap::new();
    if let PsValue::Map(entries) = params {
        for (k, v) in entries {
            let val = ps_to_val(v).map_err(|e| Failure::new(CODE_REQUEST, e.to_string()))?;
            out.insert(k.clone(), val);
        }
    }
    Ok(out)
}

pub(crate) fn ps_to_val(v: &PsValue) -> Result<Val> {
    Ok(match v {
        PsValue::Null => Val::Null,
        PsValue::Bool(b) => Val::Bool(*b),
        PsValue::Int(i) => Val::Int(*i),
        PsValue::Float(f) => Val::Float(*f),
        PsValue::String(s) => Val::Str(s.clone()),
        PsValue::Bytes(b) => Val::List(b.iter().map(|x| Val::Int(*x as i64)).collect()),
        PsValue::List(xs) => Val::List(xs.iter().map(ps_to_val).collect::<Result<_>>()?),
        PsValue::Map(m) => Val::Map(
            m.iter()
                .map(|(k, x)| Ok((k.clone(), ps_to_val(x)?)))
                .collect::<Result<_>>()?,
        ),
        PsValue::Struct { .. } => bail!("a structure cannot be used as a query parameter"),
    })
}
