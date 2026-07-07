// SPDX-License-Identifier: Apache-2.0
//! Serialise a read view to a business-key `MERGE` dump — the input a
//! consolidation rebuild feeds back to `slater-build`.
//!
//! Phase 1d consolidation is *dump-and-rebuild* (major compaction, fan-in two): the
//! current core **overlaid with the frozen delta** is written out as
//! `slater-build`-dialect Cypher and rebuilt into a fresh generation. Because the
//! serialiser reads through a [`ReadView`], pointing it at a
//! [`MergedView`](crate::read_view::MergedView) folds the delta in for free — the
//! dump already represents the consolidated state, so the builder needs no
//! delta-awareness and runs unchanged.
//!
//! # Emitted dialect (matches `slater-build`'s default business-key `MERGE` import)
//! - `CREATE INDEX FOR (n:Label) ON (n.prop);` / `CREATE INDEX FOR ()-[r:T]->() ON
//!   (r.prop);` — first, so the rebuild recreates every index (and so future writes
//!   can still resolve their business keys).
//! - `MERGE (n:Label {key: <lit>}) SET n.p = <lit>, …;` — one per node. Delta-born
//!   nodes (Phase 2c) ride the same loop: `node_count()` spans the synthetic id
//!   range and `node_record` reads a born node's label + props from the delta, so a
//!   created node is emitted (and thus survives the rebuild) exactly like a core one.
//! - `MERGE (a:LA {ka: <lit>})-[r:T]->(b:LB {kb: <lit>}) SET r.p = <lit>, …;` — one
//!   per edge, emitted from its source so each edge appears exactly once.
//!
//! # Identity: recover a business key, or refuse (never corrupt)
//! A generation does not record which property is a node's identity. We infer it
//! from the range indexes (`plan::index_for`) — the same signal the write path uses
//! — and **refuse** (a clear error, no silent data loss) when a node has no
//! range-indexed property or carries more than one label. Phase 1 assumes one
//! business identity per node; multi-key aliasing and unindexed labels are out of
//! scope until a later phase.
//!
//! # Determinism
//! Nodes and edges iterate in ascending dense-id order; a node's `SET` assignments
//! and an edge's properties are sorted by property name; the identity property is
//! the first matching range index in manifest order. So a fixed `(core, delta)`
//! serialises byte-identically — the property the consolidation golden gate rests
//! on.

use std::io::Write;

use anyhow::{bail, Context, Result};
use graph_format::ids::Value;
use graph_format::manifest::EntityKind;

use crate::exec::{val_to_value, Engine, NamedProps};
use crate::read_view::ReadView;

/// Serialise `engine`'s view to a business-key `MERGE` dump on `out`. The engine
/// must wrap the [`ReadView`] being dumped (a `MergedView` to capture the delta, or
/// a bare `Generation` for a plain export).
pub fn serialise_merge_dump<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    out: &mut impl Write,
) -> Result<()> {
    emit_index_ddl(view, out)?;
    let n = view.node_count();
    for id in 0..n {
        emit_node(engine, view, id, out)?;
    }
    for src in 0..n {
        emit_edges_from(engine, view, src, out)?;
    }
    Ok(())
}

/// `CREATE INDEX …;` for every range index, so the rebuilt generation carries the
/// same indexes forward (and business keys stay resolvable for later writes).
fn emit_index_ddl<V: ReadView>(view: &V, out: &mut impl Write) -> Result<()> {
    for ri in &view.manifest().range_indexes {
        match ri.entity {
            EntityKind::Node => writeln!(
                out,
                "CREATE INDEX FOR (n:{}) ON (n.{});",
                ri.label_or_type, ri.property
            )?,
            EntityKind::Edge => writeln!(
                out,
                "CREATE INDEX FOR ()-[r:{}]->() ON (r.{});",
                ri.label_or_type, ri.property
            )?,
        }
    }
    Ok(())
}

/// The recovered `(label, key-property, key-value)` business identity of a node, or
/// an error naming why it is unrecoverable (no range-indexed property present, or a
/// multi-label node) — consolidation refuses rather than emit an unidentifiable node.
fn node_identity(
    view: &impl ReadView,
    id: u64,
    labels: &[String],
    props: &NamedProps,
) -> Result<(String, String, Value)> {
    let [label] = labels else {
        bail!("cannot consolidate node {id}: Phase 1 requires exactly one label, found {labels:?}");
    };
    for ri in &view.manifest().range_indexes {
        if ri.entity != EntityKind::Node || &ri.label_or_type != label {
            continue;
        }
        if let Some((_, val)) = props.iter().find(|(k, _)| k == &ri.property) {
            let value = val_to_value(val).with_context(|| {
                format!(
                    "node {id} business key {}.{} is not a scalar",
                    label, ri.property
                )
            })?;
            return Ok((label.clone(), ri.property.clone(), value));
        }
    }
    bail!(
        "cannot consolidate node {id} (:{label}): no range-indexed business-key property is set — \
         add a range index on its identity property (Phase 1 identifies nodes by an indexed key)"
    )
}

/// `MERGE (n:Label {key: v}) SET n.p = v, …;` for one node — the key property
/// excluded from the `SET`, the rest sorted by name for determinism.
fn emit_node<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    id: u64,
    out: &mut impl Write,
) -> Result<()> {
    // A tombstoned node is deleted — the consolidated core must not carry it.
    if view.delta().is_tombstoned(id) {
        return Ok(());
    }
    let (labels, props) = engine.node_record(id)?;
    let (label, key, key_value) = node_identity(view, id, &labels, &props)?;
    write!(out, "MERGE (n:{label} {{{key}: {}}})", literal(&key_value))?;
    emit_set(&props, "n", Some(&key), out)?;
    writeln!(out, ";")?;
    Ok(())
}

/// Every outgoing edge of `src`, each as a business-key `MERGE` relationship. Edges
/// are emitted once, from the source, so the rebuild sees each exactly once.
fn emit_edges_from<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    src: u64,
    out: &mut impl Write,
) -> Result<()> {
    // Edges incident to a deleted node vanish with it (topology tombstones join in
    // Phase 3, but a node tombstone already removes its own incident edges here).
    if view.delta().is_tombstoned(src) {
        return Ok(());
    }
    let (slabels, sprops) = engine.node_record(src)?;
    let (sl, sk, sv) = node_identity(view, src, &slabels, &sprops)?;
    for adj in engine.outgoing_adj(src)? {
        let dst = adj.neighbour.0;
        if view.delta().is_tombstoned(dst) {
            continue;
        }
        let (dlabels, dprops) = engine.node_record(dst)?;
        let (dl, dk, dv) = node_identity(view, dst, &dlabels, &dprops)?;
        let (rtype, eprops) = engine.rel_record(adj.edge.0, adj.reltype)?;
        write!(
            out,
            "MERGE (a:{sl} {{{sk}: {}}})-[r:{rtype}]->(b:{dl} {{{dk}: {}}})",
            literal(&sv),
            literal(&dv)
        )?;
        emit_set(&eprops, "r", None, out)?;
        writeln!(out, ";")?;
    }
    Ok(())
}

/// Append ` SET <var>.<p> = <lit>, …` for every property except `exclude` (the
/// business key), sorted by name. Nothing is written when there is nothing to set.
fn emit_set(
    props: &NamedProps,
    var: &str,
    exclude: Option<&str>,
    out: &mut impl Write,
) -> Result<()> {
    let mut kept: Vec<(&String, Value)> = Vec::new();
    for (name, val) in props {
        if exclude == Some(name.as_str()) {
            continue;
        }
        let v = val_to_value(val)
            .with_context(|| format!("property {var}.{name} is not a scalar value"))?;
        kept.push((name, v));
    }
    kept.sort_by(|a, b| a.0.cmp(b.0));
    for (i, (name, v)) in kept.iter().enumerate() {
        let sep = if i == 0 { " SET" } else { "," };
        write!(out, "{sep} {var}.{name} = {}", literal(v))?;
    }
    Ok(())
}

/// Render a stored scalar [`Value`] as a `slater-build`-dialect Cypher literal that
/// round-trips exactly through the builder's parser (`parse_string` unescaping,
/// `number`/`boolean`/`null`/`list` rules). Vectors are a `MERGE`-dump non-goal.
fn literal(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format_float(*f),
        Value::Str(s) => quote_str(s),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(literal).collect();
            format!("[{}]", inner.join(", "))
        }
        // A `vecf32(...)` prop cannot ride a MERGE dump (see the vectors non-goal);
        // node_props already routes embeddings out, so this is a belt-and-braces guard.
        Value::Vector(_) => "null".to_string(),
    }
}

/// Format an `f64` so it re-parses as a float, never an int: a value with no
/// fractional/exponent part gets a `.0` suffix (the `number` rule needs a `.` or
/// `e` to be a float). Non-finite values have no dump spelling and become `null`.
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

/// Single-quote and escape a string for the builder's `sq_inner` rule, matching its
/// `parse_string` unescaping exactly (`\\`, `\'`, `\n`, `\t`, `\r`, `\0`).
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
    use crate::cache::BlockCache;
    use crate::generation::Generation;
    use crate::read_view::MergedView;
    use crate::testgen;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    #[test]
    fn literals_round_trip_and_escape() {
        assert_eq!(literal(&Value::Null), "null");
        assert_eq!(literal(&Value::Bool(true)), "true");
        assert_eq!(literal(&Value::Int(-7)), "-7");
        assert_eq!(literal(&Value::Float(2.5)), "2.5");
        // A whole-valued float keeps a decimal point so it re-parses as a float.
        assert_eq!(literal(&Value::Float(10.0)), "10.0");
        assert_eq!(format_float(f64::NAN), "null");
        assert_eq!(literal(&Value::Str("plain".into())), "'plain'");
        // Escapes match the builder's parse_string unescaping.
        assert_eq!(literal(&Value::Str("a'b\\c\nd".into())), "'a\\'b\\\\c\\nd'");
        assert_eq!(
            literal(&Value::List(vec![Value::Int(1), Value::Str("x".into())])),
            "[1, 'x']"
        );
    }

    #[test]
    fn serialise_refuses_unidentifiable_node() {
        // write_basic's Company nodes (Acme/Globex) carry a label with no range
        // index, so consolidation must refuse rather than emit an unkeyed node.
        let (root, graph, _) = testgen::write_basic("consolidate_refuse");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);
        let mut buf = Vec::new();
        let err = serialise_merge_dump(&engine, &view, &mut buf).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Company"),
            "expected a Company refusal, got: {msg}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_folds_delta_into_the_dump() {
        // A fully single-label, indexed fixture serialises cleanly, and overlaying a
        // delta patch changes the emitted node line (the dump is the merged state).
        let (root, graph) = testgen::write_indexed_people("consolidate_dump");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Pure core first.
        let core_view = MergedView::read_only(&gen);
        let mut core = Vec::new();
        serialise_merge_dump(&Engine::new(&core_view, &cache), &core_view, &mut core).unwrap();
        let core = String::from_utf8(core).unwrap();
        assert!(core.contains("CREATE INDEX FOR (n:Person) ON (n.name);"));
        assert!(
            core.contains("MERGE (n:Person {name: 'Alice'}) SET n.age = 30;"),
            "core dump:\n{core}"
        );
        // The one edge round-trips with both endpoints' business keys.
        assert!(
            core.contains("MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;"),
            "core dump:\n{core}"
        );

        // Now overlay a patch on Alice's age and re-serialise the merged view.
        let mut mem = Memtable::new();
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(0),
            [("age".to_string(), Value::Int(99))],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(
            out.contains("MERGE (n:Person {name: 'Alice'}) SET n.age = 99;"),
            "merged dump should carry the overlaid age:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_emits_a_delta_born_node() {
        // A node created only in the delta (MERGE on an absent key) must appear in the
        // consolidated dump so the rebuild carries it forward — with its business key
        // and its SET properties.
        let (root, graph) = testgen::write_indexed_people("consolidate_born");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::with_synthetic_base(gen.node_count());
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None, // delta-born: absent from the core
            [("age".to_string(), Value::Int(50))],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            out.contains("MERGE (n:Person {name: 'Dave'}) SET n.age = 50;"),
            "delta-born node must be emitted:\n{out}"
        );
        // The core people survive alongside it.
        assert!(
            out.contains("MERGE (n:Person {name: 'Alice'})"),
            "dump:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_drops_a_tombstoned_node_and_its_edge() {
        // Deleting Alice must remove both her node line and the Alice-KNOWS->Bob edge
        // from the consolidated dump — otherwise consolidation would resurrect her.
        let (root, graph) = testgen::write_indexed_people("consolidate_tombstone");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::new();
        mem.delete_node("Person", "name", Value::Str("Alice".into()), Some(0));
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            !out.contains("{name: 'Alice'}"),
            "tombstoned Alice must not appear (node or edge endpoint):\n{out}"
        );
        // Bob and Carol survive.
        assert!(
            out.contains("MERGE (n:Person {name: 'Bob'})"),
            "dump:\n{out}"
        );
        assert!(
            out.contains("MERGE (n:Person {name: 'Carol'})"),
            "dump:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
