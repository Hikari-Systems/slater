// SPDX-License-Identifier: Apache-2.0
//! The durable write path: op resolution, validation, and execution.
//!
//! Split out of `server.rs` as a child module (a pure relocation). Shared types,
//! consts and helpers stay in the parent, reached via `use super::*`; the parent
//! re-exports this module's items so sibling modules can call them by name.

use super::*;

/// Execute the parsed query on the blocking pool and return its rows already
/// encoded to PackStream (node/relationship resolution reads through the same
/// block cache, so it stays off the async reactor).
///
/// The result LRU is consulted first: a hit skips execution entirely and re-encodes
/// the cached rows for this connection's Bolt version (the cache stores the
/// version-independent `QueryResult`, so encoding — which still resolves node/rel
/// records through the block cache — is the only per-connection work). A miss
/// executes, caches the result, then encodes.
/// Resolve a write's business key `(label, key, value)` to its unique current-core
/// dense id via the label/property range index (an ISAM equality probe). `None`
/// when the property is not range-indexed for that label, the key is absent, or —
/// Phase 1 assumes a unique business key — the probe is ambiguous. The overlay's
/// dense-id read index ([`slater_delta::Memtable::by_dense`]) is built from this.
pub(crate) fn resolve_op(gen: &Generation, op: &WalOp) -> OpResolution {
    // Resolve one business key to a unique current-core dense id, or `None` when it is
    // absent (a delta-born node / endpoint, whose synthetic id the memtable allocates
    // in replay order) or non-unique/unindexed.
    let one = |(label, key, value): (&str, &str, &Value)| match resolve_business_key(
        gen, label, key, value,
    ) {
        KeyResolution::Unique(id) => Some(id),
        _ => None,
    };
    if let Some(node) = op.node_key() {
        return OpResolution::Node(one(node));
    }
    let (src, reltype, dst) = op.edge_keys().expect("node_key None ⇒ edge op");
    let src_id = one(src);
    let dst_id = one(dst);
    // An `UpsertEdge` whose endpoints are both core *and* whose edge already exists in
    // the core is an in-place property patch — resolve its core edge id so `apply`
    // routes it to `patch_core_edge` (rather than allocating a duplicate born edge).
    // A born-edge create (no core edge) or any delete resolves to `None`. Re-scanned
    // against the *current* core on every replay, so a born edge folded into a fresh
    // core (post-consolidation) correctly becomes a core-edge patch. An I/O error while
    // scanning collapses to `None` (a replay-time read failure is catastrophic anyway,
    // and this matches how endpoint resolution swallows a failed probe).
    let edge_id = match op {
        WalOp::UpsertEdge { .. } => match (src_id, dst_id, gen.reltype_id(reltype)) {
            (Some(s), Some(d), Some(rt)) => find_core_edge_id(gen, s, rt, d).unwrap_or(None),
            _ => None,
        },
        _ => None,
    };
    OpResolution::Edge {
        src: src_id,
        dst: dst_id,
        edge_id,
    }
}

/// The outcome of probing a write's business key against the current-core range
/// index. Distinguishing *absent* from *ambiguous*/*unindexed* is what lets a
/// `MERGE` create a delta-born node only when the key is genuinely new (Phase 2c).
#[derive(Clone, Copy)]
pub(crate) enum KeyResolution {
    /// Exactly one existing core node — its dense id.
    Unique(u64),
    /// The key is range-indexed but matches no core node (a `MERGE` create candidate).
    Absent,
    /// More than one core node carries the key (Phase 1 assumes a unique business key).
    Ambiguous,
    /// The `(label, key)` pair has no range index, so the write cannot be resolved.
    Unindexed,
}

/// Probe `(label, key, value)` against the label/property range index (an ISAM
/// equality probe), then **fold the core stack** over it so the write path resolves the
/// key the same way a read does (Phase 6, closing the 4.1 note (e) gap). The base
/// generation carries the index descriptor (`index_for` reads its manifest); the segment
/// fragments carry the born/patched/deleted contributions, folded oldest→newest by
/// [`CoreStack::fold_index_eq`] (each segment's `removals` sidecar suppresses the
/// base/older ids it supersedes, then its own matching ids union in — newest-wins). So a
/// `MERGE` of a business key **flushed into a segment** resolves to the segment's id (no
/// duplicate born node), a base key **deleted into a segment** resolves `Absent` (its
/// index entry is in the segment's `removals`, so a re-`MERGE` reborns it), and a key
/// **relocated by a segment patch** resolves under its new value only. The singleton
/// (no-segment) set short-circuits to the base ids, so a non-flushed graph is unchanged.
/// The overlay's dense-id read index is built from a `Unique` hit.
pub(crate) fn resolve_business_key(
    gen: &Generation,
    label: &str,
    key: &str,
    value: &Value,
) -> KeyResolution {
    let labels = [label.to_string()];
    let Some(idx) = crate::plan::index_for(gen, &labels, key) else {
        return KeyResolution::Unindexed;
    };
    let Some(reader) = gen.range_index(&idx) else {
        return KeyResolution::Unindexed;
    };
    let Ok(mut ids) = reader.lookup_eq(value) else {
        return KeyResolution::Unindexed;
    };
    let stack = gen.stack();
    if !stack.is_singleton() {
        // A fold read failure collapses to `Unindexed` — the write cannot resolve the key,
        // matching how the base probe's `Err` above is handled (a resolve-time read failure
        // is treated as "cannot resolve", never as "absent" — an `Absent` would risk a
        // duplicate born node).
        if stack.fold_index_eq(&mut ids, label, key, value).is_err() {
            return KeyResolution::Unindexed;
        }
        // The fold neither sorts nor dedups (base ids + per-segment unions), so a value
        // carried by both the base and a segment fragment would appear twice.
        ids.sort_unstable();
        ids.dedup();
    }
    match ids.as_slice() {
        [] => KeyResolution::Absent,
        [only] => KeyResolution::Unique(*only),
        _ => KeyResolution::Ambiguous,
    }
}

/// Resolve a whole batch of business-key `values` for a **fixed** `(label, key)` in one
/// merge-join sweep, returning a `KeyResolution` per input value (aligned to `values`). This
/// is the bulk-write floor from memory `bulk-delete-isam-resolve-floor`: resolving each of a
/// write batch's rows one-at-a-time re-decompresses the same ISAM leaf blocks per row (the
/// fence only skips a *segment* that cannot hold a given key — a batch of many distinct keys
/// still touches many blocks). Here the distinct values are sorted once and streamed against
/// the sorted base ISAM ([`IsamReader::lookup_eq_sorted`]) and each segment fragment
/// ([`CoreStack::fold_index_eq_batch`], carrying the oldest→newest suppress-then-union
/// semantics and the fence), so each touched block decompresses once for the whole batch.
///
/// Each value's verdict is **byte-identical** to [`resolve_business_key`] for that value: the
/// per-value base sweep equals its point `lookup_eq`, the batch fold equals the point fold,
/// and the singleton set short-circuits to the base sweep exactly as the single path does. A
/// probe of the same `(label, key)` that is unindexed, or any read failure in the sweep,
/// collapses every value to `Unindexed` (never `Absent`, so a read failure cannot manufacture
/// a duplicate born node — matching the single path).
pub(crate) fn resolve_business_keys_batch(
    gen: &Generation,
    label: &str,
    key: &str,
    values: &[&Value],
) -> Vec<KeyResolution> {
    let unindexed = || vec![KeyResolution::Unindexed; values.len()];
    let labels = [label.to_string()];
    let Some(idx) = crate::plan::index_for(gen, &labels, key) else {
        return unindexed();
    };
    let Some(reader) = gen.range_index(&idx) else {
        return unindexed();
    };
    // Base equality sweep: `ids[i]` is the base ids whose value equals `values[i]` (sorted,
    // unique — one entry per (value, id) in the base ISAM).
    let Ok(mut ids) = reader.lookup_eq_sorted(values) else {
        return unindexed();
    };
    let stack = gen.stack();
    if !stack.is_singleton() {
        if stack
            .fold_index_eq_batch(&mut ids, label, key, values)
            .is_err()
        {
            return unindexed();
        }
        // The fold unions base ids + per-segment fragment ids, so a value carried by both the
        // base and a fragment can appear twice — sort+dedup before the verdict.
        for v in &mut ids {
            v.sort_unstable();
            v.dedup();
        }
    }
    ids.iter()
        .map(|v| match v.as_slice() {
            [] => KeyResolution::Absent,
            [only] => KeyResolution::Unique(*only),
            _ => KeyResolution::Ambiguous,
        })
        .collect()
}

/// The delta snapshot + epoch to overlay when reading `gen`. The writer's delta
/// is only valid against the core generation it resolved its dense ids against;
/// after a generation swap the dense ids no longer line up, so we fail safe to the
/// pure core (empty delta) rather than mis-overlay. Phase 1c runs with
/// `reloadStrategy = exit` in practice, so this guard is defence in depth.
pub(crate) fn delta_for_read(writer: &Arc<DeltaWriter>, gen: &Arc<Generation>) -> ReadOverlay {
    if writer.core_uuid() == gen.uuid() {
        // ONE atomic read of the (snapshot, epoch) pair — see `ReadOverlay`.
        let published = writer.delta_snapshot_at();
        ReadOverlay {
            delta: published.delta,
            epoch: published.epoch,
            journal: Some(writer.touched_journal()),
        }
    } else {
        warn!(
            graph = %gen.graph(),
            "writable-layer delta resolved against a superseded generation — serving pure core"
        );
        ReadOverlay::empty()
    }
}

/// Coerce a bound value to a dense `f32` vector — the `vecf32($p)` write spelling.
///
/// Bolt has no vector type, so a driver sends an embedding as a list of numbers and it
/// arrives as a [`Val::List`] (`ps_to_val` is type-blind — it cannot know the target
/// property is vector-indexed). This is the write-side twin of the KNN path's
/// `eval_query_vector` coercion, and it keeps the two directions symmetric: a vector
/// returned to a driver is likewise rendered as a float list.
pub(crate) fn coerce_vecf32(v: Value, what: &str) -> std::result::Result<Value, Failure> {
    let items = match v {
        Value::Vector(_) => return Ok(v),
        Value::List(items) => items,
        other => {
            return Err(Failure::new(
                CODE_REQUEST,
                format!(
                    "{what}: vecf32() needs a list of numbers, got {}",
                    other.type_name()
                ),
            ))
        }
    };
    let mut out = Vec::with_capacity(items.len());
    for (i, x) in items.into_iter().enumerate() {
        let n = match x {
            Value::Float(f) => f,
            Value::Int(i) => i as f64,
            other => {
                return Err(Failure::new(
                    CODE_REQUEST,
                    format!(
                        "{what}: vecf32() elements must be numbers, got {}",
                        other.type_name()
                    ),
                ))
            }
        };
        // The Bolt front door: a driver can send a `NaN`/`±inf` `Float64` directly (no
        // `log()` needed). Reject it here through the one shared finiteness gate so a
        // non-finite component never enters an embedding via the wire (HIK-134).
        let c = graph_format::pq::finite_f32(i, n as f32)
            .map_err(|e| Failure::new(CODE_REQUEST, format!("{what}: {e}")))?;
        out.push(c);
    }
    Ok(Value::Vector(out))
}

/// Reject an embedding whose dimension disagrees with the index it is written to.
///
/// Both KNN arms hard-error on a dim mismatch, and a bad row would otherwise ride the T2
/// flush into a segment and the rebuild into the next generation before anyone noticed.
/// The write is the one place to catch it cheaply, and the one place that can still
/// report it to the client. A vector on an *unindexed* `(label, property)` is unconstrained
/// — it is an ordinary inline value, and the core admits those at any width.
pub(crate) fn validate_vector_dims(
    ops: &[WalOp],
    gen: &Generation,
) -> std::result::Result<(), Failure> {
    let indexes = &gen.manifest().vector_indexes;
    if indexes.is_empty() {
        return Ok(());
    }
    let check = |label: &str, prop: &str, v: &Value| -> std::result::Result<(), Failure> {
        let Value::Vector(xs) = v else {
            return Ok(());
        };
        let Some(d) = indexes
            .iter()
            .find(|d| d.label == label && d.property == prop)
        else {
            return Ok(());
        };
        if xs.len() != d.dim as usize {
            return Err(Failure::new(
                CODE_REQUEST,
                format!(
                    "the vector index on (:{label} {{{prop}}}) is {}-dimensional, but the value \
                     assigned to {prop} has {} dimensions",
                    d.dim,
                    xs.len()
                ),
            ));
        }
        Ok(())
    };
    for op in ops {
        match op {
            WalOp::UpsertNode { label, patches, .. }
            | WalOp::ReplaceNode { label, patches, .. } => {
                for (prop, v) in patches {
                    check(label, prop, v)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Evaluate a Phase 1c write's constant expression (a literal or a parameter) to a
/// storable [`Value`] against the query's parameters.
pub(crate) fn write_value(
    e: &parser::ast::Expr,
    params: &HashMap<String, Val>,
    what: &str,
) -> std::result::Result<Value, Failure> {
    use parser::ast::Expr;
    // `vecf32($p)` is the one call `ensure_constant` admits: its value is knowable only
    // once the parameter is bound, so unlike the all-literal form it cannot be folded at
    // lowering. Anything else non-constant was rejected there.
    if let Some(arg) = parser::as_vecf32_arg(e) {
        let val = match arg {
            Expr::Param(name) => params.get(name).ok_or_else(|| {
                Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
            })?,
            _ => unreachable!("lower_write_statement folds or rejects vecf32 over {what}"),
        };
        let v = crate::exec::val_to_value(val).ok_or_else(|| {
            Failure::new(
                CODE_REQUEST,
                format!("{what} is not a storable scalar value"),
            )
        })?;
        return coerce_vecf32(v, what);
    }
    let val = match e {
        Expr::Literal(v) => return Ok(v.clone()),
        Expr::Param(name) => params.get(name).ok_or_else(|| {
            Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
        })?,
        _ => unreachable!("lower_write_statement rejects non-constant {what}"),
    };
    crate::exec::val_to_value(val).ok_or_else(|| {
        Failure::new(
            CODE_REQUEST,
            format!("{what} is not a storable scalar value"),
        )
    })
}

/// Build the durable node WAL op sequence for a write statement, evaluating each value
/// through `eval` (constant/parameter for a plain write, per-row for a write-`UNWIND`).
/// Shared by the plain and batch paths so they cannot diverge. `SET n.p = v` /
/// `SET n += {map}` fold into `UpsertNode` patches (source order, LWW); `SET n = {map}`
/// emits a `ReplaceNode`; `REMOVE n.p` a `RemoveNodeProps`; `DELETE` a `DeleteNode`.
/// A statement that mixes a replace with further items yields several ops that
/// group-commit atomically. Label mutations (Stage 5) are still rejected by name.
/// Fold a list of `SET` items (identity `label`/`key`/`value` fixed) into a WAL op
/// sequence: `var.prop = v` / `var += {map}` accumulate into `UpsertNode` patches (source
/// order, LWW); `var = {map}` emits a `ReplaceNode`; `var:Label` a `SetNodeLabels`. When
/// `ensure_nonempty` and the items produce nothing, a no-op upsert is emitted (so a MERGE
/// still create-if-absent's its node). Shared by the main `SET`, the `ON CREATE`/`ON MATCH`
/// blocks, and `CREATE`.
pub(crate) fn fold_set_items(
    items: &[parser::ast::SetItem],
    label: &str,
    key: &str,
    value: &Value,
    ensure_nonempty: bool,
    eval: impl Fn(&parser::ast::Expr, &str) -> std::result::Result<Value, Failure>,
) -> std::result::Result<Vec<WalOp>, Failure> {
    use parser::ast::SetItem;
    let upsert = |patches: Vec<(String, Value)>| WalOp::UpsertNode {
        label: label.to_string(),
        key: key.to_string(),
        value: value.clone(),
        patches,
    };
    let mut ops: Vec<WalOp> = Vec::new();
    let mut pending: Vec<(String, Value)> = Vec::new();
    let mut added_labels: Vec<String> = Vec::new();
    for item in items {
        match item {
            // Patching the anchor key's value is allowed — it relocates the node in the
            // index (the "moved indexed value" overlay); the delta identity stays fixed.
            SetItem::Prop { prop, value: expr } => {
                pending.push((prop.clone(), eval(expr, "a SET value")?));
            }
            SetItem::MergeMap(map) => {
                for (k, expr) in map {
                    pending.push((k.clone(), eval(expr, "a merge-map value")?));
                }
            }
            SetItem::ReplaceMap(map) => {
                let patches = replace_map_patches(map, &eval)?;
                pending.clear();
                ops.push(WalOp::ReplaceNode {
                    label: label.to_string(),
                    key: key.to_string(),
                    value: value.clone(),
                    patches,
                });
            }
            // Label additions are independent of the property patches; collect them into
            // one SetNodeLabels op emitted after the patch flush.
            SetItem::AddLabels(labels) => added_labels.extend(labels.iter().cloned()),
        }
    }
    if !pending.is_empty() {
        ops.push(upsert(pending));
    }
    if !added_labels.is_empty() {
        ops.push(WalOp::SetNodeLabels {
            label: label.to_string(),
            key: key.to_string(),
            value: value.clone(),
            added: added_labels,
            removed: Vec::new(),
        });
    }
    if ops.is_empty() && ensure_nonempty {
        ops.push(upsert(Vec::new()));
    }
    Ok(ops)
}

pub(crate) fn build_node_wal_ops(
    stmt: &parser::ast::WriteStmt,
    key_value: &Value,
    eval: impl Fn(&parser::ast::Expr, &str) -> std::result::Result<Value, Failure>,
) -> std::result::Result<Vec<WalOp>, Failure> {
    use parser::ast::{RemoveItem, WriteOp};
    let label = stmt.label.clone();
    let key = stmt.key.clone();
    let value = key_value.clone();
    match &stmt.op {
        // The main SET fold emits at least one op (a no-op upsert when empty) so a MERGE
        // create-if-absent's its node and the write acks.
        WriteOp::Set(items) => fold_set_items(items, &label, &key, &value, true, eval),
        WriteOp::Remove(items) => {
            let mut props = Vec::new();
            let mut removed_labels = Vec::new();
            for item in items {
                match item {
                    RemoveItem::Prop(p) => {
                        if p == &stmt.key {
                            return Err(Failure::new(
                                CODE_REQUEST,
                                format!(
                                    "cannot REMOVE the business-key property '{p}' — it is the \
                                     node's identity"
                                ),
                            ));
                        }
                        props.push(p.clone());
                    }
                    RemoveItem::Labels(labels) => removed_labels.extend(labels.iter().cloned()),
                }
            }
            let mut ops = Vec::new();
            if !props.is_empty() {
                ops.push(WalOp::RemoveNodeProps {
                    label: label.clone(),
                    key: key.clone(),
                    value: value.clone(),
                    props,
                });
            }
            if !removed_labels.is_empty() {
                ops.push(WalOp::SetNodeLabels {
                    label,
                    key,
                    value,
                    added: Vec::new(),
                    removed: removed_labels,
                });
            }
            debug_assert!(!ops.is_empty(), "REMOVE names at least one prop or label");
            Ok(ops)
        }
        // A node DELETE tombstones the node; the topology overlay then suppresses its
        // incident edges. DELETE conformance (Stage 2) — a plain DELETE of a connected
        // node — is enforced by the caller after resolution.
        WriteOp::Delete { .. } => Ok(vec![WalOp::DeleteNode { label, key, value }]),
    }
}

/// Validate the label mutations in a write's op sequence against the graph and the
/// resolved node:
///  - a `SET n:Label` naming a label absent from the core symbol table is rejected (a
///    brand-new label has no core id, so the read overlay could not honour it — the
///    pre-existing-label subset ships first);
///  - `REMOVE n:<identity-label>` on a **delta-born** node is rejected (Decision C): its
///    label comes from its identity, so dropping it would leave the node label-less. On
///    an existing **core** node the drop is allowed (it still resolves by dense id).
///
/// `resolved` is the node's dense id; a born id is at or above the core node count.
pub(crate) fn validate_label_ops(
    ops: &[WalOp],
    resolved: Option<u64>,
    gen: &Generation,
    stmt: &parser::ast::WriteStmt,
) -> std::result::Result<(), Failure> {
    let is_born = resolved.is_some_and(|id| id >= gen.node_count());
    for op in ops {
        let WalOp::SetNodeLabels { added, removed, .. } = op else {
            continue;
        };
        for l in added {
            if gen.label_id(l).is_none() {
                return Err(Failure::new(
                    CODE_REQUEST,
                    format!(
                        "cannot add label ':{l}' — it is not defined in the graph (only \
                         pre-existing labels can be set)"
                    ),
                ));
            }
        }
        if is_born && removed.iter().any(|l| l == &stmt.label) {
            return Err(Failure::new(
                CODE_REQUEST,
                format!(
                    "cannot REMOVE the identity label ':{}' from a newly-created node",
                    stmt.label
                ),
            ));
        }
    }
    Ok(())
}

/// Evaluate a `SET n = {map}` replace map into storable patches. The map may re-set the
/// anchor key (which relocates the node in the index, like any indexed-value patch); a
/// map that omits the key keeps it — the reader re-seeds it from the delta identity.
pub(crate) fn replace_map_patches(
    map: &[(String, parser::ast::Expr)],
    eval: impl Fn(&parser::ast::Expr, &str) -> std::result::Result<Value, Failure>,
) -> std::result::Result<Vec<(String, Value)>, Failure> {
    let mut patches = Vec::with_capacity(map.len());
    for (k, expr) in map {
        patches.push((k.clone(), eval(expr, "a replace-map value")?));
    }
    Ok(patches)
}

/// Whether node `id` still has any relationship over the merged view (core + the
/// writer's current delta). Used to enforce openCypher DELETE conformance: a plain
/// `DELETE` of a node that still has relationships is an error — only `DETACH DELETE`
/// removes them. Both `outgoing_adj` and `incoming_adj` are overlay-aware, so an edge
/// a prior write already tombstoned (or an edge to an already-deleted node) is not
/// counted; the check therefore sees the *live* incident set at write time.
pub(crate) fn node_has_relationships(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    id: u64,
) -> std::result::Result<bool, Failure> {
    let delta = DeltaSnapshot::from_memtable(writer.snapshot());
    let view = MergedView::new(gen, delta);
    let cache = BlockCache::new(1 << 16);
    let engine = crate::exec::Engine::new(&view, &cache);
    node_has_relationships_via(&engine, id)
}

/// The engine-driven core of [`node_has_relationships`]: does `id` have any incident
/// relationship in the engine's overlaid view? Uses the short-circuit existence probe
/// ([`Engine::has_incident_edge`]) so a high-degree hub stops at its first live edge instead
/// of materialising the whole adjacency `Vec`. Factored out so the batched DELETE path can
/// hoist **one** engine (over the shared per-batch cache) out of its per-row loop rather than
/// rebuild a throwaway 64 KiB `BlockCache` + engine — and fully re-decode a hub — every row.
pub(crate) fn node_has_relationships_via<V: ReadView>(
    engine: &Engine<'_, V>,
    id: u64,
) -> std::result::Result<bool, Failure> {
    engine.has_incident_edge(id).map_err(|e: anyhow::Error| {
        Failure::new(
            CODE_EXECUTION,
            format!("check the node's incident relationships: {e:#}"),
        )
    })
}

/// The error a plain (non-`DETACH`) `DELETE` raises when its node still has
/// relationships — openCypher requires the edges be removed first.
pub(crate) fn delete_has_relationships_error() -> Failure {
    Failure::new(
        CODE_EXECUTION,
        "Cannot delete node, because it still has relationships. To delete it and its \
         relationships, use DETACH DELETE."
            .into(),
    )
}

/// One parsed write statement, ready to execute. The three write shapes differ only in
/// which `execute_*` they dispatch to; carrying them in one owned enum lets a single
/// helper move any of them onto a blocking thread.
pub(crate) enum WriteJob {
    /// Boxed: a `WriteStmt` is much the biggest of the three, and every `WriteJob` is
    /// moved into a blocking task.
    Node(Box<parser::ast::WriteStmt>),
    Create(parser::ast::CreateStmt),
    Edge(parser::ast::EdgeWriteStmt),
}

impl WriteJob {
    /// Run the write. Called on a blocking thread, never on the reactor.
    fn run(
        self,
        writer: &Arc<DeltaWriter>,
        gen: &Generation,
        params: &HashMap<String, Val>,
    ) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
        match self {
            WriteJob::Node(stmt) => execute_write(writer, gen, &stmt, params),
            WriteJob::Create(stmt) => execute_create(writer, gen, &stmt, params),
            WriteJob::Edge(stmt) => execute_edge_write(writer, gen, &stmt, params),
        }
    }
}

/// Execute one write statement **off the reactor**, under a concurrency cap.
///
/// A write is not cheap and it is not pure CPU: it resolves business keys against the
/// core (ISAM `pread` + zstd decompress — a network round trip on an S3/GCS backend),
/// materialises adjacency, then appends to the WAL and **fsyncs**. Running that inline in
/// the async `handle_request` — as the three RUN write arms did — parks a tokio *reactor*
/// worker for the whole of it, so one slow write stalls every other connection that
/// worker is driving and a handful of concurrent writes deafen the server entirely. Read
/// execution ([`run_query`]), consolidation and the delta-maintenance rungs have always
/// been on `spawn_blocking`; the write arms were the odd ones out (module doc, top of
/// file). So:
///
/// * the whole statement — resolve, adjacency, WAL append, fsync — moves to a blocking
///   thread, leaving the reactor free to keep driving every other connection's IO;
/// * `write_limit` caps how many writes execute **at once**. The cap is the point, not a
///   detail: every mutation of one graph is serialised behind that graph's single
///   [`DeltaWriter`] lock, so a bare `spawn_blocking` would merely relocate the problem —
///   a write flood would hand tokio's 512-thread blocking pool an unbounded queue of
///   tasks that immediately park on a mutex they cannot get, and *read queries, which run
///   on that same pool*, would starve behind them. A small cap keeps the pool free.
///   Permits above a handful buy nothing anyway (they only queue on the writer lock); the
///   handful that is there pays for itself because key resolution — the expensive,
///   IO-bound part — happens *outside* the lock, and separate graphs have separate
///   writers. Waiters park asynchronously: no thread, no reactor worker, and their number
///   is bounded by `server.maxConnections` (a writer is authenticated by construction);
/// * the permit is moved **into** the blocking closure, so it is released when the write
///   actually finishes rather than when a hung-up client cancels the await (a cancelled
///   `spawn_blocking` still runs to completion — releasing the permit early would let a
///   client who disconnects mid-write overrun the cap).
///
/// Write ordering is unaffected: one connection's requests are handled strictly in
/// sequence, and the order in which concurrent connections' writes land was — and still
/// is — decided by the single writer lock, not by this gate.
///
/// A panicked or aborted write task **fails closed**: the caller reports a failure, never
/// a SUCCESS. Durability is unchanged — the ack is still written only after
/// `DeltaWriter`'s fsync has returned.
pub(crate) async fn execute_write_off_reactor(
    ctx: &Arc<ConnCtx>,
    writer: &Arc<DeltaWriter>,
    gen: &Arc<Generation>,
    job: WriteJob,
    params: HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let permit = ctx
        .write_limit
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| Failure::new(CODE_EXECUTION, "server is shutting down".into()))?;

    let writer = writer.clone();
    let gen = gen.clone();
    tokio::task::spawn_blocking(move || {
        // Held until the write is done, not until the caller stops waiting for it.
        let _permit = permit;
        job.run(&writer, gen.as_ref(), &params)
    })
    .await
    .map_err(|e| {
        // A panicked/aborted write is not acknowledged: the client is told it failed.
        warn!(error = %e, "write task did not complete");
        Failure::new(CODE_EXECUTION, "the write did not complete".into())
    })?
}

/// Execute one durable write: build the WAL op sequence from the parsed statement +
/// parameters, resolve the anchor's business key to a current-core dense id, and hand
/// the ops to the writer (WAL append + fsync commit + memtable apply + publish) as one
/// group commit. A statement lowers to several ops only when it mixes a replace-all with
/// further SET items; they commit atomically. Returns an empty result — read-back is a
/// separate `MATCH … RETURN` over the overlaid view.
pub(crate) fn execute_write(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::WriteStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    use parser::ast::WriteOp;
    if stmt.ret.is_some() {
        return Err(Failure::new(
            CODE_REQUEST,
            "RETURN after a write is not yet supported; issue a separate MATCH … RETURN to read \
             back the written values"
                .into(),
        ));
    }
    // A leading `UNWIND <list> AS r` is a batched (group-committed) write.
    if stmt.unwind.is_some() {
        return execute_write_batch(writer, gen, stmt, params);
    }
    let key_value = write_value(&stmt.key_value, params, "the anchor business-key value")?;
    let ops = build_node_wal_ops(stmt, &key_value, |e, what| write_value(e, params, what))?;
    // Every op in a statement shares the anchor key, so one resolution serves them all.
    // A non-DELETE op is "set-like" (addresses an existing node, or MERGE-creates one).
    let mut ops = ops;
    // MERGE `ON CREATE SET` / `ON MATCH SET`: whether the MERGE creates or matches is
    // decided by the pre-write state, so compute it before appending the conditional ops.
    if stmt.upsert && (!stmt.on_create.is_empty() || !stmt.on_match.is_empty()) {
        let created = merge_creates_node(writer, gen, &stmt.label, &stmt.key, &key_value);
        let items = if created {
            &stmt.on_create
        } else {
            &stmt.on_match
        };
        ops.extend(fold_set_items(
            items,
            &stmt.label,
            &stmt.key,
            &key_value,
            false,
            |e, what| write_value(e, params, what),
        )?);
    }
    // After the conditional fold, so an `ON CREATE SET` embedding is checked too.
    validate_vector_dims(&ops, gen)?;
    let is_set = !matches!(stmt.op, WriteOp::Delete { .. });
    let first = ops.first().expect("a node write yields at least one op");
    let resolved = resolve_node_op(writer, gen, first, is_set, stmt.upsert)?;
    validate_label_ops(&ops, resolved, gen, stmt)?;
    // DELETE conformance: a plain (non-DETACH) DELETE errors if the node still has any
    // relationship. `resolved` is the node's dense id (a delete never returns `None`).
    if let WriteOp::Delete { detach: false, .. } = &stmt.op {
        if let Some(id) = resolved {
            if node_has_relationships(writer, gen, id)? {
                return Err(delete_has_relationships_error());
            }
        }
    }
    let batch: Vec<(WalOp, OpResolution)> = ops
        .into_iter()
        .map(|op| (op, OpResolution::Node(resolved)))
        .collect();
    writer
        .write_batch(&batch)
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Whether a MERGE on `(label, key, value)` **creates** a new node (vs matching an
/// existing one). The node exists if the current core carries the key uniquely, or a
/// prior write already made it a delta-born node; otherwise this MERGE creates it.
/// Computed against the pre-write state (so it must be called before the op is applied).
pub(crate) fn merge_creates_node(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    label: &str,
    key: &str,
    value: &Value,
) -> bool {
    merge_creates_node_from(
        writer,
        resolve_business_key(gen, label, key, value),
        label,
        key,
        value,
    )
}

/// The core half of [`merge_creates_node`] over an already-resolved `KeyResolution` — so the
/// batch path can resolve every row's key in one merge-join sweep and decide create-vs-match
/// per row without a second per-row core probe.
pub(crate) fn merge_creates_node_from(
    writer: &Arc<DeltaWriter>,
    resolution: KeyResolution,
    label: &str,
    key: &str,
    value: &Value,
) -> bool {
    match resolution {
        KeyResolution::Unique(_) => false, // an existing core node — matched
        KeyResolution::Absent => writer.born_synthetic_in_delta(label, key, value).is_none(),
        // Ambiguous / unindexed will error at `resolve_node_op`; treat as "not created".
        _ => false,
    }
}

/// Execute a `CREATE (n:Label {props})`: designate the range-indexed property as the
/// business key and unconditionally create (born-upsert) the node with the remaining
/// properties. Errors if no inline property is the label's range-indexed identity.
pub(crate) fn execute_create(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::CreateStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    if stmt.ret.is_some() {
        return Err(Failure::new(
            CODE_REQUEST,
            "RETURN after a write is not yet supported; issue a separate MATCH … RETURN".into(),
        ));
    }
    // Evaluate every inline property, then pick the business key: the label's
    // range-indexed property (lowest core label id breaks a tie among indexes).
    let mut props: Vec<(String, Value)> = Vec::with_capacity(stmt.props.len());
    for (name, expr) in &stmt.props {
        props.push((
            name.clone(),
            write_value(expr, params, "a CREATE property")?,
        ));
    }
    let key = gen
        .manifest()
        .range_indexes
        .iter()
        .find(|ri| {
            ri.entity == graph_format::manifest::EntityKind::Node
                && ri.label_or_type == stmt.label
                && props.iter().any(|(p, _)| p == &ri.property)
        })
        .map(|ri| ri.property.clone())
        .ok_or_else(|| {
            Failure::new(
                CODE_REQUEST,
                format!(
                    "cannot CREATE (:{}): none of its properties is the label's range-indexed \
                     business key — add a range index, or use MERGE with an inline key",
                    stmt.label
                ),
            )
        })?;
    let key_pos = props
        .iter()
        .position(|(p, _)| p == &key)
        .expect("key present");
    let (_, key_value) = props.remove(key_pos);
    let op = WalOp::UpsertNode {
        label: stmt.label.clone(),
        key: key.clone(),
        value: key_value,
        patches: props,
    };
    // Born-create (upsert semantics): resolve as a set-like op with create-on-absent.
    let resolved = resolve_node_op(writer, gen, &op, true, true)?;
    writer
        .write(op, OpResolution::Node(resolved))
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable CREATE failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Does this statement mutate the graph, and so require a `write` grant?
///
/// Every arm of the write grammar must be listed here: node writes (`MERGE` /
/// `MATCH … SET` / `MATCH … DELETE`, plain or under a write-`UNWIND`), relationship writes
/// (`MERGE (a)-[r:R]->(b) [SET …]` / `MATCH (a)-[r:R]->(b) DELETE r`), and the
/// `CALL slater.consolidate()` admin trigger, which rewrites the served generation.
/// Matching on the enum rather than sniffing the query text means a new write statement
/// cannot be added without the compiler forcing a decision here.
pub(crate) fn statement_mutates(stmt: &parser::ast::Statement) -> bool {
    match stmt {
        parser::ast::Statement::Write(_)
        | parser::ast::Statement::Create(_)
        | parser::ast::Statement::WriteEdge(_)
        | parser::ast::Statement::Consolidate => true,
        parser::ast::Statement::Read(_) => false,
    }
}

/// Gate a parsed statement on the caller's grants for `graph`.
///
/// Reads are already gated at graph selection (`Acl::can_read`); this adds the write gate.
/// A `read` grant does **not** imply the right to mutate, so switching on `delta.enabled`
/// cannot silently promote every existing reader into a writer.
pub(crate) fn authorize_statement(
    acl: &Acl,
    user: &str,
    graph: &str,
    stmt: &parser::ast::Statement,
) -> std::result::Result<(), Failure> {
    if statement_mutates(stmt) && !acl.can_write(user, graph) {
        return Err(Failure::new(
            CODE_FORBIDDEN,
            format!("write access to graph '{graph}' is not granted to this user"),
        ));
    }
    Ok(())
}

/// Resolve a node write's business key to its dense-id context for the WAL op. `Unique`
/// → the core id; a MERGE-create (`is_set && upsert`) on an `Absent` key → a born
/// synthetic id (reusing one already flushed to L0, else `None` to allocate); a DELETE
/// or a `MATCH … SET` of a born node → its synthetic id resolved across the whole delta;
/// every other absent / ambiguous / unindexed case is a clear error. Shared by the single
/// and batched (write-UNWIND) node write paths so their semantics cannot drift.
///
/// Every `Absent` arm consults the delta, not just the core: a delta-born node is a real,
/// readable node, so `MERGE`, `DELETE` and `SET` must all be able to name it.
pub(crate) fn resolve_node_op(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    op: &WalOp,
    is_set: bool,
    upsert: bool,
) -> std::result::Result<Option<u64>, Failure> {
    let (label, key, value) = op.node_key().expect("resolve_node_op is for node ops only");
    resolve_node_op_from(
        writer,
        resolve_business_key(gen, label, key, value),
        label,
        key,
        value,
        is_set,
        upsert,
    )
}

/// The core half of [`resolve_node_op`] over an already-resolved `KeyResolution` — the
/// delta/born-id decision that does not touch the ISAM. The batch path resolves every row's
/// business key in one merge-join sweep (the bulk-write floor) and then routes each row's
/// `KeyResolution` through here, so the batched and single write paths still share one set of
/// create/update/delete semantics (they cannot drift).
pub(crate) fn resolve_node_op_from(
    writer: &Arc<DeltaWriter>,
    resolution: KeyResolution,
    label: &str,
    key: &str,
    value: &Value,
    is_set: bool,
    upsert: bool,
) -> std::result::Result<Option<u64>, Failure> {
    Ok(match resolution {
        KeyResolution::Unique(id) => Some(id),
        // MERGE create: a key absent from the core is a delta-born node — reuse an id
        // already flushed to L0, else `None` allocates a fresh one.
        KeyResolution::Absent if is_set && upsert => {
            writer.born_synthetic_for_identity(label, key, value)
        }
        // DELETE of a delta-born node: resolve its synthetic id across the whole delta so
        // the tombstone suppresses it (even if flushed to L0). Absent everywhere → error.
        KeyResolution::Absent if !is_set => {
            match writer.born_synthetic_in_delta(label, key, value) {
                Some(id) => Some(id),
                None => {
                    return Err(Failure::new(
                        CODE_EXECUTION,
                        format!(
                            "no {label}({key} = …) node to delete: the business key matches no \
                             existing node"
                        ),
                    ))
                }
            }
        }
        // A `MATCH … SET` (update-only) whose key matches no core node may still name a
        // **delta-born** node: it exists and reads back like any other, so an update has
        // to resolve it across the whole delta exactly as the DELETE arm above does.
        // Absent from the core *and* the delta → the key names nothing.
        KeyResolution::Absent => match writer.born_synthetic_in_delta(label, key, value) {
            Some(id) => Some(id),
            None => {
                return Err(Failure::new(
                    CODE_EXECUTION,
                    format!(
                        "no {label}({key} = …) node to update: the business key matches no \
                         existing node (use MERGE to create it)"
                    ),
                ))
            }
        },
        KeyResolution::Ambiguous => {
            return Err(Failure::new(
                CODE_EXECUTION,
                format!(
                    "the business key {label}({key} = …) matches more than one node — writes \
                     require a unique business key"
                ),
            ))
        }
        KeyResolution::Unindexed => {
            return Err(Failure::new(
                CODE_EXECUTION,
                format!(
                    "cannot write {label}({key} = …): the business key must be range-indexed to \
                     resolve it"
                ),
            ))
        }
    })
}

/// Evaluate a write-UNWIND per-row value expression: a literal, a parameter, the row
/// variable `var` itself, or `var.field` (a field of the current row map). Anything else
/// is rejected — a batched write's values are the bulk-import subset, not arbitrary
/// expressions. Returns the storable [`Value`].
pub(crate) fn eval_row_value(
    e: &parser::ast::Expr,
    var: &str,
    row: &Val,
    params: &HashMap<String, Val>,
    what: &str,
) -> std::result::Result<Value, Failure> {
    use parser::ast::Expr;
    // `SET n.embedding = vecf32(r.emb)` — the batched spelling. The argument is itself a
    // row reference, so evaluate it through this same restricted grammar and coerce.
    if let Some(arg) = parser::as_vecf32_arg(e) {
        let v = eval_row_value(arg, var, row, params, what)?;
        return coerce_vecf32(v, what);
    }
    let val: Val = match e {
        Expr::Literal(v) => return Ok(v.clone()),
        Expr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
        })?,
        Expr::Var(v) if v == var => row.clone(),
        Expr::Property(base, field) => match base.as_ref() {
            Expr::Var(v) if v == var => match row {
                Val::Map(m) => m
                    .iter()
                    .find(|(k, _)| k == field)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Val::Null),
                _ => {
                    return Err(Failure::new(
                        CODE_REQUEST,
                        format!(
                            "the UNWIND row is not a map, so {var}.{field} cannot supply {what}"
                        ),
                    ))
                }
            },
            _ => {
                return Err(Failure::new(
                    CODE_REQUEST,
                    format!(
                        "{what} may reference only the UNWIND variable '{var}' (as {var}.field)"
                    ),
                ))
            }
        },
        _ => {
            return Err(Failure::new(
                CODE_REQUEST,
                format!("{what} must be a literal, a parameter, or {var}.field in a batched write"),
            ))
        }
    };
    crate::exec::val_to_value(&val).ok_or_else(|| {
        Failure::new(
            CODE_REQUEST,
            format!("{what} is not a storable scalar value"),
        )
    })
}

/// Execute a **batched** node write (`UNWIND <list> AS r MATCH|MERGE (n:L {k: …}) …`):
/// evaluate the source list, build one WAL op per row (its business key + SET values
/// evaluated against that row), resolve each against the core, and apply the whole batch
/// under a single group commit ([`DeltaWriter::write_batch`] — one fsync, one publish).
/// Atomic: if any row fails to evaluate or resolve, the batch is rejected before it is
/// committed. NB: resolution is against the core ⊕ the delta *as of the batch start*, so
/// a within-batch create-then-delete of the same new key is not resolved (independent
/// rows — the bulk-import case — are unaffected).
pub(crate) fn execute_write_batch(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::WriteStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    use parser::ast::{Expr, WriteOp};
    let (source, var) = stmt
        .unwind
        .as_ref()
        .expect("execute_write_batch requires an UNWIND");
    // The UNWIND source is a parameter list (the bulk-import shape, `UNWIND $rows AS r`).
    let list: Val = match source {
        Expr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
        })?,
        _ => {
            return Err(Failure::new(
                CODE_REQUEST,
                "the UNWIND source of a batched write must be a parameter list (e.g. \
                 `UNWIND $rows AS r`)"
                    .into(),
            ))
        }
    };
    let rows = match list {
        Val::List(items) => items,
        _ => {
            return Err(Failure::new(
                CODE_REQUEST,
                "the UNWIND source of a batched write is not a list".into(),
            ))
        }
    };
    // Evaluate every row's anchor business-key value up front, then resolve the whole batch's
    // keys against the core in **one merge-join sweep** (`resolve_business_keys_batch`) rather
    // than a per-row ISAM point probe — the bulk-write floor (memory
    // `bulk-delete-isam-resolve-floor`). The `(label, key)` is fixed across the batch, so only
    // the value varies: dedup the values, sweep the distinct set once, then fan each row's
    // resolution back out. The per-row `KeyResolution` is byte-identical to what the single
    // path would compute (the core probe reads `gen` only — the accumulating delta cannot
    // change it), so the born-id / create-vs-match decisions below are unchanged.
    let key_values: Vec<Value> = rows
        .iter()
        .map(|row| {
            eval_row_value(
                &stmt.key_value,
                var,
                row,
                params,
                "the anchor business-key value",
            )
        })
        .collect::<std::result::Result<_, _>>()?;
    let row_res: Vec<KeyResolution> = {
        // Distinct values in `cmp_key` order (the ISAM order the sweep needs), with each row
        // mapped to its distinct slot.
        let mut order: Vec<usize> = (0..key_values.len()).collect();
        order.sort_by(|&a, &b| key_values[a].cmp_key(&key_values[b]));
        let mut distinct: Vec<&Value> = Vec::new();
        let mut row_to_distinct = vec![0usize; key_values.len()];
        for &ri in &order {
            if distinct
                .last()
                .is_none_or(|last| !last.cmp_key(&key_values[ri]).is_eq())
            {
                distinct.push(&key_values[ri]);
            }
            row_to_distinct[ri] = distinct.len() - 1;
        }
        let resolved = resolve_business_keys_batch(gen, &stmt.label, &stmt.key, &distinct);
        row_to_distinct.iter().map(|&d| resolved[d]).collect()
    };

    // Hoist a single overlaid view + block cache + engine for the whole batch so the plain-DELETE
    // conformance probe below reuses them across every row, instead of allocating a throwaway
    // 64 KiB `BlockCache` (and re-decoding a hub's adjacency) per row. No op mutates the memtable
    // inside this loop — the ops accumulate and commit in the single `write_batch` after it — so
    // the pre-loop snapshot is the pre-batch state every per-row check would otherwise re-snapshot,
    // byte-identical. Built unconditionally (cheap: `Engine::new` allocates nothing material and
    // the cache stays empty until the first probe reads a block); the probe fires only for a plain
    // DELETE batch.
    let batch_delta = DeltaSnapshot::from_memtable(writer.snapshot());
    let batch_view = MergedView::new(gen, batch_delta);
    let batch_cache = BlockCache::new(1 << 16);
    let batch_engine = crate::exec::Engine::new(&batch_view, &batch_cache);

    let mut ops: Vec<(WalOp, OpResolution)> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let key_value = &key_values[i];
        let resolution = row_res[i];
        let mut row_ops = build_node_wal_ops(stmt, key_value, |e, what| {
            eval_row_value(e, var, row, params, what)
        })?;
        // MERGE ON CREATE / ON MATCH, per row (create-vs-match against the pre-batch state).
        if stmt.upsert && (!stmt.on_create.is_empty() || !stmt.on_match.is_empty()) {
            let created =
                merge_creates_node_from(writer, resolution, &stmt.label, &stmt.key, key_value);
            let items = if created {
                &stmt.on_create
            } else {
                &stmt.on_match
            };
            row_ops.extend(fold_set_items(
                items,
                &stmt.label,
                &stmt.key,
                key_value,
                false,
                |e, what| eval_row_value(e, var, row, params, what),
            )?);
        }
        validate_vector_dims(&row_ops, gen)?;
        let is_set = !matches!(stmt.op, WriteOp::Delete { .. });
        debug_assert!(!row_ops.is_empty(), "a node write yields at least one op");
        let resolved = resolve_node_op_from(
            writer,
            resolution,
            &stmt.label,
            &stmt.key,
            key_value,
            is_set,
            stmt.upsert,
        )?;
        validate_label_ops(&row_ops, resolved, gen, stmt)?;
        // DELETE conformance, per row: a plain DELETE errors if the row's node still
        // has a relationship (the batch is all-DELETE or all-SET, so no edge this batch
        // creates precedes the check).
        if let WriteOp::Delete { detach: false, .. } = &stmt.op {
            if let Some(id) = resolved {
                if node_has_relationships_via(&batch_engine, id)? {
                    return Err(delete_has_relationships_error());
                }
            }
        }
        for op in row_ops {
            ops.push((op, OpResolution::Node(resolved)));
        }
    }
    writer
        .write_batch(&ops)
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable batch write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Resolve an edge-write endpoint's business key to a current-core dense id.
/// `Unique` → the id; `Absent` → `None` (a `MERGE` auto-creates a delta-born endpoint,
/// a `DELETE` no-ops if it is also not a born node); ambiguous / unindexed is a clear
/// error, exactly as the node write path.
pub(crate) fn resolve_endpoint(
    gen: &Generation,
    ep: &parser::ast::EndpointPat,
    value: &Value,
) -> std::result::Result<Option<u64>, Failure> {
    match resolve_business_key(gen, &ep.label, &ep.key, value) {
        KeyResolution::Unique(id) => Ok(Some(id)),
        KeyResolution::Absent => Ok(None),
        KeyResolution::Ambiguous => Err(Failure::new(
            CODE_EXECUTION,
            format!(
                "the business key {}({} = …) matches more than one node — writes require a \
                 unique business key",
                ep.label, ep.key
            ),
        )),
        KeyResolution::Unindexed => Err(Failure::new(
            CODE_EXECUTION,
            format!(
                "cannot write a relationship to {}({} = …): the business key must be range-indexed \
                 to resolve it",
                ep.label, ep.key
            ),
        )),
    }
}

/// The core edge id of `src -[reltype]-> dst` if the core already carries that edge,
/// else `None`. This is both the `MERGE` idempotency check (a re-`MERGE` of an existing
/// core edge must not add a duplicate delta-born edge) and the resolver for an in-place
/// core-edge property patch (the id keys the patch overlay). Scans only the source's
/// core outgoing adjacency (bounded by its out-degree) over an **empty-delta** view, so
/// it sees core edges only — a born duplicate is prevented by the memtable's identity
/// idempotency, and a patch must land on the genuine core edge id, never a synthetic one.
pub(crate) fn find_core_edge_id(
    gen: &Generation,
    src: u64,
    reltype: u32,
    dst: u64,
) -> std::result::Result<Option<u64>, Failure> {
    let cache = BlockCache::new(1 << 16);
    let view = MergedView::read_only(gen);
    let engine = crate::exec::Engine::new(&view, &cache);
    // Short-circuit at the first matching out-edge instead of materialising the source's whole
    // out-adjacency `Vec` to `find()` one edge — a hub source is never fully decoded. The
    // empty-delta (`read_only`) view keeps this core-only, so it returns the genuine core edge id.
    engine.find_outgoing_edge(src, reltype, dst).map_err(|e| {
        Failure::new(
            CODE_EXECUTION,
            format!("check for an existing relationship: {e:#}"),
        )
    })
}

/// Execute one durable relationship write (Phase 3c): resolve both endpoints, build
/// the WAL edge op, and hand it to the writer. A `MERGE` of an edge that already
/// exists in the core is an idempotent no-op; the relationship type must already
/// exist (the traversal overlay maps it to a core reltype id).
pub(crate) fn execute_edge_write(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::EdgeWriteStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    use parser::ast::EdgeWriteOp;
    // The reltype must pre-exist: the read overlay resolves a born edge's type through
    // the core symbol table, so a brand-new type would be invisible to traversal.
    let Some(reltype_id) = gen.reltype_id(&stmt.reltype) else {
        return Err(Failure::new(
            CODE_EXECUTION,
            format!(
                "cannot write a :{} relationship: the relationship type must already exist in the \
                 graph",
                stmt.reltype
            ),
        ));
    };
    let src_value = write_value(&stmt.src.key_value, params, "the source business-key value")?;
    let dst_value = write_value(
        &stmt.dst.key_value,
        params,
        "the destination business-key value",
    )?;
    // Evaluate the optional `SET r.p = …` property patches (empty for a bare MERGE or a
    // DELETE). They are carried on the WAL op and stored on the delta-born edge.
    let mut patches = Vec::with_capacity(stmt.sets.len());
    for (prop, expr) in &stmt.sets {
        patches.push((
            prop.clone(),
            write_value(expr, params, "a relationship SET value")?,
        ));
    }
    // Core-only resolution first (`None` = absent from the core): the duplicate check
    // below must run against genuine core dense ids, never a delta-born synthetic id.
    let src_core = resolve_endpoint(gen, &stmt.src, &src_value)?;
    let dst_core = resolve_endpoint(gen, &stmt.dst, &dst_value)?;

    // A MERGE of an edge whose endpoints are both existing core nodes may already exist
    // in the core. If it does, a bare re-MERGE is an idempotent no-op, and a
    // `SET r.p = …` is an **in-place property patch** of that core edge (resolved to its
    // core edge id, which routes the write to `patch_core_edge`). If either endpoint is
    // delta-born there can be no matching core edge, so no check is needed.
    let mut core_edge_id = None;
    if stmt.op == EdgeWriteOp::Create {
        if let (Some(s), Some(d)) = (src_core, dst_core) {
            core_edge_id = find_core_edge_id(gen, s, reltype_id, d)?;
            if core_edge_id.is_some() && patches.is_empty() {
                // Bare re-MERGE of an existing core edge: nothing to write.
                return Ok((Vec::new(), Vec::new()));
            }
        }
    }

    // Resolve the WAL op's endpoints: an endpoint absent from the core but already born
    // and flushed to an L0 level reuses its synthetic id (Phase 4c-B) rather than
    // allocating a duplicate born endpoint; a still-`None` endpoint is a fresh born node
    // (MERGE) or a no-op (DELETE), exactly as before.
    let src = src_core
        .or_else(|| writer.born_synthetic_for_identity(&stmt.src.label, &stmt.src.key, &src_value));
    let dst = dst_core
        .or_else(|| writer.born_synthetic_for_identity(&stmt.dst.label, &stmt.dst.key, &dst_value));

    let op = match stmt.op {
        EdgeWriteOp::Create => WalOp::UpsertEdge {
            src_label: stmt.src.label.clone(),
            src_key: stmt.src.key.clone(),
            src_value,
            reltype: stmt.reltype.clone(),
            dst_label: stmt.dst.label.clone(),
            dst_key: stmt.dst.key.clone(),
            dst_value,
            patches,
        },
        EdgeWriteOp::Delete => WalOp::DeleteEdge {
            src_label: stmt.src.label.clone(),
            src_key: stmt.src.key.clone(),
            src_value,
            reltype: stmt.reltype.clone(),
            dst_label: stmt.dst.label.clone(),
            dst_key: stmt.dst.key.clone(),
            dst_value,
        },
    };
    // `edge_id` is `Some` only for a core-edge patch (a Create whose edge exists in the
    // core); a born-edge create and every delete leave it `None`.
    writer
        .write(
            op,
            OpResolution::Edge {
                src,
                dst,
                edge_id: core_edge_id,
            },
        )
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}
