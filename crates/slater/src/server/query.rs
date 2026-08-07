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

/// An estimate of a result's resident footprint, used to charge it against the
/// result-cache budget. Exactness is impossible (allocator size classes and slack
/// are invisible from here) but it must never *under*-count: the budget is what
/// bounds RSS, so a value that looks smaller than it is lets the pool overshoot.
///
/// Counts the `QueryResult` struct, the `columns`/`rows` `Vec` **capacities**, each
/// column `String`'s capacity, and each row's `Vec<Val>` capacity plus the heap its
/// values own. Capacity, not length, throughout — the slack a `String`/`Vec` holds
/// is resident whether or not it is used.
pub(crate) fn estimate_result_bytes(r: &QueryResult) -> usize {
    let cols: usize = r.columns.capacity() * std::mem::size_of::<String>()
        + r.columns.iter().map(|c| c.capacity()).sum::<usize>();
    let rows: usize = r.rows.capacity() * std::mem::size_of::<Vec<Val>>()
        + r.rows
            .iter()
            .map(|row| {
                // `val_bytes` charges each *occupied* slot plus its heap; the row
                // `Vec`'s unused capacity is resident too, so charge that slack.
                row.capacity().saturating_sub(row.len()) * std::mem::size_of::<Val>()
                    + row.iter().map(val_bytes).sum::<usize>()
            })
            .sum::<usize>();
    std::mem::size_of::<QueryResult>() + cols + rows
}

/// A single value's resident footprint: its inline enum slot plus the heap it owns.
pub(crate) fn val_bytes(v: &Val) -> usize {
    std::mem::size_of::<Val>() + val_heap_bytes(v)
}

/// Bytes `v` owns on the heap, beyond its own inline `size_of::<Val>()` slot.
/// Strings, lists, vectors and maps are counted by allocation **capacity**, and
/// nested containers recurse. Mirrors `value_heap_bytes` in
/// `graph_format::isam` and `slater_build::merge_build` — same shape, same reason
/// (HIK-101: a flat/`len`-based charge made a block of long strings look far
/// smaller than it was and the budget could be badly overshot).
///
/// `Node`/`Rel`/`Point` and the temporals own nothing on the heap — they are
/// plain scalars — so the enum slot already covers them.
///
/// CONTRACT: every `0` arm above asserts the variant stores its payload **inline**.
/// The exhaustive match forces a decision when a variant is *added*, but not when an
/// existing one gains an indirection — boxing `Path`, or making `Str` an `Arc<str>`,
/// would leave its arm silently under-charging and re-arm exactly the bug this
/// function exists to fix. Changing a variant's representation means revisiting its
/// arm. `result_byte_estimate_covers_string_and_container_capacity` pins
/// `size_of::<Val>()` as a floor so a shrinking enum slot cannot quietly erode the
/// charge either.
fn val_heap_bytes(v: &Val) -> usize {
    const SLOT: usize = std::mem::size_of::<Val>();
    match v {
        Val::Null | Val::Bool(_) | Val::Int(_) | Val::Float(_) => 0,
        Val::Str(s) => s.capacity(),
        Val::List(xs) => xs.capacity() * SLOT + xs.iter().map(val_heap_bytes).sum::<usize>(),
        Val::Vector(xs) => xs.capacity() * std::mem::size_of::<f32>(),
        Val::Map(m) => {
            m.capacity() * std::mem::size_of::<(String, Val)>()
                + m.iter()
                    .map(|(k, x)| k.capacity() + val_heap_bytes(x))
                    .sum::<usize>()
        }
        Val::Node(_) | Val::Rel { .. } | Val::Point { .. } => 0,
        Val::Path { nodes, rels } => {
            nodes.capacity() * std::mem::size_of::<u64>()
                + rels.capacity() * SLOT
                + rels.iter().map(val_heap_bytes).sum::<usize>()
        }
        Val::Date(_) | Val::Time(_) | Val::DateTime(_) | Val::Duration(_) => 0,
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
        // The temporal structs decode to strings — see `ps_temporal_to_val`. Every
        // other structure (Node, Relationship, Path, Point) is a *result* shape that
        // no client has any business sending back as an input.
        PsValue::Struct { tag, fields } => match ps_temporal_to_val(*tag, fields)? {
            Some(val) => val,
            None => bail!(
                "a {} structure cannot be used as a query parameter",
                struct_tag_name(*tag)
            ),
        },
    })
}

/// Decode a Bolt temporal structure into a [`Val`]. Returns `Ok(None)` if the tag is
/// not a temporal, so the caller can report it as an unusable parameter shape.
///
/// **Why these become `Val::Str` and not `Val::Date`/`Val::DateTime`.** Those variants
/// exist, but they are constructor/compute-only: [`crate::exec::val_to_value`] returns
/// `None` for all four because *the on-disk format cannot store a temporal*. Decoding a
/// parameter into one would parse cleanly and then die on the write with "not a storable
/// scalar value" — the failure simply moves one layer later. Giving temporals a storable
/// representation means a new `Value` wire tag in `graph_format::wire`, which reinterprets
/// existing bytes and is a format-version project in its own right, not something to
/// smuggle in on the parameter path.
///
/// An ISO-8601 string is a genuinely good target rather than a consolation prize:
/// - it is storable, and range-indexable, as an ordinary `Value::Str`;
/// - `<`, `>` and `ORDER BY` over UTC ISO-8601 are lexicographic, so they order correctly;
/// - it carries sub-second precision, which `Val::DateTime` (whole seconds — see
///   [`crate::temporal`]) does not.
///
/// **Fractional seconds are always exactly 9 digits, and that is load-bearing.** Omitting
/// a zero fraction would break the ordering property outright: `'…:00Z' < '…:00.5Z'` is
/// *false*, because `'.'` (0x2E) sorts below `'Z'` (0x5A). Fixed width keeps the compare
/// total. Clients that hold less precision truncate on the way back in — Python's
/// `datetime.fromisoformat` accepts all 9 and keeps its native 6.
///
/// Zoned forms are normalised to UTC and suffixed `Z`, so two instants are comparable
/// regardless of the offset the client happened to send.
fn ps_temporal_to_val(tag: u8, fields: &[PsValue]) -> Result<Option<Val>> {
    /// A Bolt temporal field, which is always an integer.
    fn int_at(fields: &[PsValue], i: usize, what: &str) -> Result<i64> {
        match fields.get(i) {
            Some(PsValue::Int(n)) => Ok(*n),
            _ => bail!("malformed Bolt {what} structure: field {i} is not an integer"),
        }
    }
    /// `seconds` since the epoch + `nanos` → `YYYY-MM-DDTHH:MM:SS.fffffffff`, with the
    /// caller's suffix. Nanos are normalised into the second so a client that sends
    /// `nanoseconds >= 1e9` (or negative) cannot produce a malformed string.
    fn iso_datetime(seconds: i64, nanos: i64, suffix: &str) -> Result<String> {
        let secs = seconds
            .checked_add(nanos.div_euclid(1_000_000_000))
            .ok_or_else(|| anyhow::anyhow!("Bolt datetime is out of range"))?;
        let nanos = nanos.rem_euclid(1_000_000_000) as u32;
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
            .ok_or_else(|| anyhow::anyhow!("Bolt datetime is out of range"))?;
        Ok(format!("{}{suffix}", dt.format("%Y-%m-%dT%H:%M:%S%.9f")))
    }

    let val = match tag {
        // Date: [days since epoch].
        TAG_DATE => {
            let days = int_at(fields, 0, "Date")?;
            let d = chrono::DateTime::<chrono::Utc>::from_timestamp(
                days.checked_mul(86_400)
                    .ok_or_else(|| anyhow::anyhow!("Bolt Date is out of range"))?,
                0,
            )
            .ok_or_else(|| anyhow::anyhow!("Bolt Date is out of range"))?;
            Val::Str(d.format("%Y-%m-%d").to_string())
        }
        // LocalTime: [nanoOfDay]. No zone, so no suffix.
        TAG_LOCAL_TIME => Val::Str(iso_time(int_at(fields, 0, "LocalTime")?)?),
        // Time: [nanoOfDay, tzOffsetSeconds] — rotated to UTC within the day.
        TAG_TIME => {
            let nanos = int_at(fields, 0, "Time")?;
            let offset = int_at(fields, 1, "Time")?;
            let utc = nanos
                .checked_sub(
                    offset
                        .checked_mul(1_000_000_000)
                        .ok_or_else(|| anyhow::anyhow!("Bolt Time offset is out of range"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("Bolt Time is out of range"))?;
            Val::Str(format!(
                "{}Z",
                iso_time(utc.rem_euclid(86_400_000_000_000))?
            ))
        }
        // LocalDateTime: [seconds, nanoseconds]. Zone-free, so no `Z`.
        TAG_LOCAL_DATETIME => Val::Str(iso_datetime(
            int_at(fields, 0, "LocalDateTime")?,
            int_at(fields, 1, "LocalDateTime")?,
            "",
        )?),
        // DateTime (Bolt >= 5): [seconds (UTC), nanoseconds, tzOffsetSeconds]. This is
        // what an official driver sends for a timezone-aware datetime, which is what
        // every `datetime.now(timezone.utc)` produces — i.e. the common case.
        TAG_DATETIME => Val::Str(iso_datetime(
            int_at(fields, 0, "DateTime")?,
            int_at(fields, 1, "DateTime")?,
            "Z",
        )?),
        // DateTime (legacy, Bolt < 5): same shape, but `seconds` is *local* — subtract
        // the offset to reach UTC. Reachable on the 4.4 / 4.1 fallbacks.
        TAG_LEGACY_DATETIME => {
            let local = int_at(fields, 0, "DateTime")?;
            let offset = int_at(fields, 2, "DateTime")?;
            Val::Str(iso_datetime(
                local
                    .checked_sub(offset)
                    .ok_or_else(|| anyhow::anyhow!("Bolt DateTime is out of range"))?,
                int_at(fields, 1, "DateTime")?,
                "Z",
            )?)
        }
        // DateTimeZoneId (Bolt >= 5): [seconds (UTC), nanoseconds, tzId]. The zone id is
        // presentational once the instant is UTC, so it is dropped rather than resolved.
        TAG_DATETIME_ZONE_ID => Val::Str(iso_datetime(
            int_at(fields, 0, "DateTimeZoneId")?,
            int_at(fields, 1, "DateTimeZoneId")?,
            "Z",
        )?),
        // DateTimeZoneId (legacy): `seconds` is local, and recovering UTC needs the IANA
        // rules for `tzId` at that instant. Slater carries no tz database, and guessing
        // would silently shift an instant by up to a day. Refuse, and name the fix.
        TAG_LEGACY_DATETIME_ZONE_ID => bail!(
            "a zoned DateTime naming an IANA time zone cannot be used as a query parameter \
             on Bolt 4.x (Slater carries no time-zone database); send a UTC-offset datetime, \
             or connect over Bolt 5.x where the wire value is already UTC"
        ),
        // Duration: [months, days, seconds, nanoseconds] → ISO-8601. Months and days stay
        // unfolded because neither has a fixed length in seconds.
        TAG_DURATION => {
            let (months, days, seconds, nanos) = (
                int_at(fields, 0, "Duration")?,
                int_at(fields, 1, "Duration")?,
                int_at(fields, 2, "Duration")?,
                int_at(fields, 3, "Duration")?,
            );
            let secs = seconds
                .checked_add(nanos.div_euclid(1_000_000_000))
                .ok_or_else(|| anyhow::anyhow!("Bolt Duration is out of range"))?;
            Val::Str(format!(
                "P{}M{}DT{}.{:09}S",
                months,
                days,
                secs,
                nanos.rem_euclid(1_000_000_000)
            ))
        }
        _ => return Ok(None),
    };
    Ok(Some(val))
}

/// `nanoOfDay` → `HH:MM:SS.fffffffff`. Nine fractional digits, for the ordering reason
/// given on [`ps_temporal_to_val`].
fn iso_time(nano_of_day: i64) -> Result<String> {
    if !(0..86_400_000_000_000).contains(&nano_of_day) {
        bail!("Bolt time-of-day {nano_of_day} is outside a single day");
    }
    let (secs, nanos) = (nano_of_day / 1_000_000_000, nano_of_day % 1_000_000_000);
    Ok(format!(
        "{:02}:{:02}:{:02}.{:09}",
        secs / 3_600,
        (secs / 60) % 60,
        secs % 60,
        nanos
    ))
}

/// A human name for a structure tag, for the "cannot be used as a query parameter"
/// message — an operator seeing `0x4E` has to go looking, `Node` they do not.
fn struct_tag_name(tag: u8) -> String {
    match tag {
        TAG_NODE => "Node".into(),
        TAG_RELATIONSHIP => "Relationship".into(),
        TAG_UNBOUND_REL => "UnboundRelationship".into(),
        TAG_PATH => "Path".into(),
        TAG_POINT2D => "Point2D".into(),
        _ => format!("0x{tag:02X}"),
    }
}

#[cfg(test)]
mod temporal_param_tests {
    use super::*;

    fn decode(tag: u8, fields: Vec<PsValue>) -> Result<Val> {
        ps_to_val(&PsValue::Struct { tag, fields })
    }
    fn decoded_str(tag: u8, fields: Vec<PsValue>) -> String {
        match decode(tag, fields) {
            Ok(Val::Str(s)) => s,
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// The shape an official driver sends for a timezone-aware `datetime` — which is
    /// what `datetime.now(timezone.utc)` produces, i.e. every temporal Graphiti writes.
    /// Before this decode existed, `ps_to_val` refused it and *every* write failed at
    /// the parameter decoder, before any Cypher was parsed.
    #[test]
    fn a_bolt_5_datetime_decodes_to_a_utc_iso_string() {
        assert_eq!(
            decoded_str(
                TAG_DATETIME,
                vec![PsValue::Int(1), PsValue::Int(500_000_000), PsValue::Int(0)],
            ),
            "1970-01-01T00:00:01.500000000Z"
        );
    }

    /// Bolt 4.x carries *local* seconds plus the offset; UTC is `seconds - offset`.
    /// Reachable on the 4.4 / 4.1 fallbacks, so it must agree with the 5.x form.
    #[test]
    fn a_legacy_datetime_is_shifted_by_its_offset_to_match_the_bolt_5_form() {
        let legacy = decoded_str(
            TAG_LEGACY_DATETIME,
            vec![
                PsValue::Int(3_601), // 01:00:01 local
                PsValue::Int(0),
                PsValue::Int(3_600), // +01:00
            ],
        );
        let modern = decoded_str(
            TAG_DATETIME,
            vec![PsValue::Int(1), PsValue::Int(0), PsValue::Int(3_600)],
        );
        assert_eq!(legacy, "1970-01-01T00:00:01.000000000Z");
        assert_eq!(
            legacy, modern,
            "the two wire spellings must agree on the instant"
        );
    }

    /// The ordering property the whole string representation exists to preserve. A
    /// fraction-less spelling would invert this: `'.'` (0x2E) sorts below `'Z'` (0x5A),
    /// so `'…:00Z' < '…:00.5Z'` is *false*. Graphiti's `ORDER BY e.valid_at DESC` and
    /// `WHERE e.valid_at <= $reference_time` both depend on it holding.
    #[test]
    fn fixed_width_fractions_keep_the_lexicographic_order_correct() {
        let zero = decoded_str(
            TAG_DATETIME,
            vec![PsValue::Int(0), PsValue::Int(0), PsValue::Int(0)],
        );
        let half = decoded_str(
            TAG_DATETIME,
            vec![PsValue::Int(0), PsValue::Int(500_000_000), PsValue::Int(0)],
        );
        let next = decoded_str(
            TAG_DATETIME,
            vec![PsValue::Int(1), PsValue::Int(0), PsValue::Int(0)],
        );
        assert!(zero < half && half < next, "{zero} < {half} < {next}");
        for s in [&zero, &half, &next] {
            assert_eq!(s.len(), "1970-01-01T00:00:00.000000000Z".len());
        }
    }

    /// Out-of-range nanoseconds fold into the second rather than producing a string
    /// like `…:00.1500000000Z` that no ISO-8601 parser would accept.
    #[test]
    fn nanoseconds_beyond_a_second_normalise_into_the_seconds_field() {
        assert_eq!(
            decoded_str(
                TAG_DATETIME,
                vec![
                    PsValue::Int(0),
                    PsValue::Int(1_500_000_000),
                    PsValue::Int(0)
                ],
            ),
            "1970-01-01T00:00:01.500000000Z"
        );
    }

    #[test]
    fn date_local_time_and_local_date_time_decode() {
        assert_eq!(decoded_str(TAG_DATE, vec![PsValue::Int(1)]), "1970-01-02");
        assert_eq!(
            decoded_str(TAG_LOCAL_TIME, vec![PsValue::Int(3_601_000_000_000)]),
            "01:00:01.000000000"
        );
        // Zone-free, so no `Z` — a LocalDateTime names no instant.
        assert_eq!(
            decoded_str(TAG_LOCAL_DATETIME, vec![PsValue::Int(5), PsValue::Int(0)]),
            "1970-01-01T00:00:05.000000000"
        );
    }

    /// A zoned Time rotates within the day, wrapping rather than escaping `[0, 24h)`.
    #[test]
    fn a_zoned_time_normalises_to_utc_and_wraps_within_the_day() {
        // 00:30:00+01:00 is 23:30:00Z on the previous day; only the time survives.
        assert_eq!(
            decoded_str(
                TAG_TIME,
                vec![PsValue::Int(1_800_000_000_000), PsValue::Int(3_600)],
            ),
            "23:30:00.000000000Z"
        );
    }

    #[test]
    fn duration_keeps_months_and_days_unfolded() {
        // Neither a month nor a day has a fixed length in seconds, so folding them
        // would change the value.
        assert_eq!(
            decoded_str(
                TAG_DURATION,
                vec![
                    PsValue::Int(2),
                    PsValue::Int(3),
                    PsValue::Int(3_604),
                    PsValue::Int(0),
                ],
            ),
            "P2M3DT3604.000000000S"
        );
    }

    /// Refused rather than guessed at: recovering UTC needs the IANA rules for the zone
    /// at that instant, and being wrong shifts the instant by up to a day.
    #[test]
    fn a_legacy_zone_id_datetime_is_refused_with_an_actionable_message() {
        let err = decode(
            TAG_LEGACY_DATETIME_ZONE_ID,
            vec![
                PsValue::Int(0),
                PsValue::Int(0),
                PsValue::str("Europe/London"),
            ],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("time-zone database"), "got: {err}");
        assert!(err.contains("Bolt 5.x"), "must name the fix. Got: {err}");
    }

    /// Result-only shapes stay refused — but by name, not as a bare "a structure".
    #[test]
    fn a_node_parameter_is_still_refused_and_names_the_shape() {
        let err = decode(TAG_NODE, vec![PsValue::Int(1)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Node"), "got: {err}");
    }

    /// A malformed structure is a clean error, never a panic — these bytes are
    /// attacker-controlled.
    #[test]
    fn a_malformed_temporal_structure_errors_rather_than_panicking() {
        for fields in [
            vec![],
            vec![PsValue::str("not an int")],
            vec![PsValue::Int(0)], // DateTime needs three fields
        ] {
            assert!(decode(TAG_DATETIME, fields).is_err());
        }
        // A day-of-time outside [0, 24h) is rejected rather than silently wrapped.
        assert!(decode(TAG_LOCAL_TIME, vec![PsValue::Int(-1)]).is_err());
        assert!(decode(TAG_LOCAL_TIME, vec![PsValue::Int(86_400_000_000_000)]).is_err());
    }
}
