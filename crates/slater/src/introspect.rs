// SPDX-License-Identifier: Apache-2.0
//! Read-only introspection / metadata queries — the fixed set of statements a
//! graph-browser GUI (Neo4j Browser, Memgraph Lab) fires on connect to populate
//! its schema/sidebar panels.
//!
//! slater's query engine is a strict read-only Cypher subset that rejects every
//! `CALL db.*` (except the vector KNN procedure) and every `SHOW …` statement. So
//! rather than widen that grammar, the Bolt `RUN` handler ([`crate::server`])
//! recognises this curated set of statements and answers them here, straight from
//! the in-memory generation [`Manifest`]. Every answer is derived from
//! already-resident metadata (labels, reltypes, property keys, index descriptors,
//! node/edge counts) — there is no graph scan, so these stay O(metadata).
//!
//! Two dialects are covered:
//! - **Neo4j**: `CALL dbms.components()`, `CALL db.labels()`,
//!   `CALL db.relationshipTypes()`, `CALL db.propertyKeys()`, `CALL db.indexes()`,
//!   `CALL db.schema.visualization()`, `SHOW DATABASES`, `SHOW INDEXES`,
//!   `SHOW CONSTRAINTS`, `SHOW FUNCTIONS`, `SHOW PROCEDURES`, `SHOW TRANSACTIONS`.
//! - **Memgraph**: `SHOW STORAGE INFO`, `SHOW INDEX INFO`, `SHOW CONSTRAINT INFO`,
//!   `SHOW TRIGGERS`, `SHOW VERSION`.

use graph_format::manifest::{EntityKind, Manifest};

use crate::bolt::packstream::PsValue;

/// A synthesised result: the field names and the rows (each a row of values).
pub(crate) type Rows = (Vec<String>, Vec<Vec<PsValue>>);

fn cols(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}
fn s(x: impl Into<String>) -> PsValue {
    PsValue::String(x.into())
}
fn strlist<I: IntoIterator<Item = String>>(xs: I) -> PsValue {
    PsValue::List(xs.into_iter().map(PsValue::String).collect())
}

// ── graph-agnostic (server-level) ─────────────────────────────────────────────

/// `CALL dbms.components()` — version/edition probe. We answer as a modern Neo4j
/// kernel so the official drivers and Browser enable their 5.x + multi-database
/// code paths (the `SHOW DATABASES` selector that surfaces our several graphs).
pub(crate) fn dbms_components() -> Rows {
    (
        cols(&["name", "versions", "edition"]),
        vec![vec![
            s("Neo4j Kernel"),
            strlist(["5.4.0".to_string()]),
            s("enterprise"),
        ]],
    )
}

/// `CALL slater.diagnostics()` / `SHOW SERVER DIAGNOSTICS` — the gated load-test
/// health snapshot, rendered as a two-column `metric`/`value` table (the same
/// shape `SHOW STORAGE INFO` uses). The caller (`server`) builds the ordered
/// `(name, value)` rows from the live [`crate::diag::Diagnostics`] snapshot; this
/// just frames them for the wire. `value` is a mixed Int/Float column so a driver
/// returns each metric as its native type.
pub(crate) fn server_diagnostics(rows: &[(String, PsValue)]) -> Rows {
    (
        cols(&["metric", "value"]),
        rows.iter()
            .map(|(k, v)| vec![s(k.clone()), v.clone()])
            .collect(),
    )
}

/// `SHOW VERSION` (Memgraph) — single-column version string.
pub(crate) fn show_version() -> Rows {
    (
        cols(&["version"]),
        vec![vec![s(concat!("Slater ", env!("CARGO_PKG_VERSION")))]],
    )
}

/// `SHOW DATABASES` — lists the graphs this user may read, one of which is flagged
/// `default`/`home`. This is what populates a browser's database selector, letting
/// the user pick a graph (which the driver then sends as the `db` field).
pub(crate) fn show_databases(
    dbs: &[(String, bool)],
    address: &str,
    writable: impl Fn(&str) -> bool,
) -> Rows {
    let columns = cols(&[
        "name",
        "type",
        "aliases",
        "access",
        "address",
        "role",
        "writer",
        "requestedStatus",
        "currentStatus",
        "statusMessage",
        "default",
        "home",
        "constituents",
    ]);
    let rows = dbs
        .iter()
        .map(|(name, is_default)| {
            // `access` / `writer` reflect whether the writable layer is active for this
            // graph (a per-graph delta writer exists), not a hardcoded read-only.
            let rw = writable(name);
            vec![
                s(name.clone()),
                s("standard"),
                PsValue::List(vec![]),
                s(if rw { "read-write" } else { "read-only" }),
                s(address),
                s("primary"),
                PsValue::Bool(rw),
                s("online"),
                s("online"),
                s(""),
                PsValue::Bool(*is_default),
                PsValue::Bool(*is_default),
                PsValue::List(vec![]),
            ]
        })
        .collect();
    (columns, rows)
}

/// An empty result with the given columns (for the `SHOW …` statements we accept
/// but have nothing to report: constraints, functions, triggers, transactions).
pub(crate) fn empty(columns: &[&str]) -> Rows {
    (cols(columns), vec![])
}

/// `SHOW LICENSE INFO` (Memgraph) — Memgraph Lab probes this on connect to decide
/// whether Enterprise features (incl. multiple databases / multi-tenancy) are
/// available. We report a valid Enterprise license so Lab surfaces its database
/// selector and drives graph selection via `USE <graph>`.
pub(crate) fn show_license_info() -> Rows {
    (
        cols(&["license info", "value"]),
        vec![
            vec![s("organization_name"), s("Slater")],
            vec![s("license_type"), s("enterprise")],
            vec![s("is_valid"), s("true")],
            vec![s("valid_until"), s("")],
            vec![s("memory_limit"), s("0")],
        ],
    )
}

/// `SHOW REPLICATION ROLE` (Memgraph) — a standalone instance is the `main`.
pub(crate) fn show_replication_role() -> Rows {
    (cols(&["replication role"]), vec![vec![s("main")]])
}

/// `SHOW DATABASE` (Memgraph, singular) — the connection's current database. Reports
/// the sticky `USE <graph>` selection, or `memgraph` when none has been chosen yet.
pub(crate) fn show_database(current: Option<&str>) -> Rows {
    (
        cols(&["Name"]),
        vec![vec![s(current.unwrap_or("memgraph").to_string())]],
    )
}

/// `SHOW DATABASES` in **Memgraph** multi-tenancy format: a single `Name` column
/// (Neo4j's `SHOW DATABASES` shape, [`show_databases`], differs — many columns, lower
/// case `name`). Memgraph Lab reads this to populate its database dropdown.
pub(crate) fn show_databases_memgraph(dbs: &[(String, bool)]) -> Rows {
    (
        cols(&["Name"]),
        dbs.iter().map(|(n, _)| vec![s(n.clone())]).collect(),
    )
}

/// `SHOW PROCEDURES` — the only callable procedure slater exposes.
pub(crate) fn show_procedures() -> Rows {
    (
        cols(&["name", "description", "mode"]),
        vec![vec![
            s("db.idx.vector.queryNodes"),
            s("Approximate/exact vector KNN over a node vector index."),
            s("READ"),
        ]],
    )
}

// ── graph-scoped (need a selected graph's manifest) ───────────────────────────

pub(crate) fn db_labels(m: &Manifest) -> Rows {
    (
        cols(&["label"]),
        m.labels.iter().map(|l| vec![s(l.clone())]).collect(),
    )
}

pub(crate) fn db_relationship_types(m: &Manifest) -> Rows {
    (
        cols(&["relationshipType"]),
        m.reltypes.iter().map(|t| vec![s(t.clone())]).collect(),
    )
}

pub(crate) fn db_property_keys(m: &Manifest) -> Rows {
    (
        cols(&["propertyKey"]),
        m.property_keys.iter().map(|p| vec![s(p.clone())]).collect(),
    )
}

/// Flattened view of every index in the manifest, dialect-independent.
struct IdxView {
    name: String,
    kind: &'static str,   // "RANGE" | "VECTOR"
    entity: &'static str, // "NODE" | "RELATIONSHIP"
    label: String,
    property: String,
    provider: &'static str,
}

fn index_views(m: &Manifest) -> Vec<IdxView> {
    let mut out = Vec::with_capacity(m.range_indexes.len() + m.vector_indexes.len());
    for r in &m.range_indexes {
        out.push(IdxView {
            name: r.name.clone(),
            kind: "RANGE",
            entity: match r.entity {
                EntityKind::Node => "NODE",
                EntityKind::Edge => "RELATIONSHIP",
            },
            label: r.label_or_type.clone(),
            property: r.property.clone(),
            provider: "range-1.0",
        });
    }
    for v in &m.vector_indexes {
        out.push(IdxView {
            name: format!("vector_{}_{}", v.label, v.property),
            kind: "VECTOR",
            entity: "NODE",
            label: v.label.clone(),
            property: v.property.clone(),
            provider: "vector-2.0",
        });
    }
    out
}

/// `SHOW INDEXES` (Neo4j 5.x).
pub(crate) fn show_indexes(m: &Manifest) -> Rows {
    let columns = cols(&[
        "id",
        "name",
        "state",
        "populationPercent",
        "type",
        "entityType",
        "labelsOrTypes",
        "properties",
        "indexProvider",
        "owningConstraint",
        "lastRead",
        "readCount",
    ]);
    let rows = index_views(m)
        .into_iter()
        .enumerate()
        .map(|(i, ix)| {
            vec![
                PsValue::Int(i as i64),
                s(ix.name),
                s("ONLINE"),
                PsValue::Float(100.0),
                s(ix.kind),
                s(ix.entity),
                strlist([ix.label]),
                strlist([ix.property]),
                s(ix.provider),
                PsValue::Null,
                PsValue::Null,
                PsValue::Int(0),
            ]
        })
        .collect();
    (columns, rows)
}

/// `CALL db.indexes()` (Neo4j 4.x, deprecated but still issued by older clients).
pub(crate) fn db_indexes(m: &Manifest) -> Rows {
    let columns = cols(&[
        "id",
        "name",
        "state",
        "populationPercent",
        "uniqueness",
        "type",
        "entityType",
        "labelsOrTypes",
        "properties",
        "provider",
    ]);
    let rows = index_views(m)
        .into_iter()
        .enumerate()
        .map(|(i, ix)| {
            vec![
                PsValue::Int(i as i64),
                s(ix.name),
                s("ONLINE"),
                PsValue::Float(100.0),
                s("NONUNIQUE"),
                s(ix.kind),
                s(ix.entity),
                strlist([ix.label]),
                strlist([ix.property]),
                s(ix.provider),
            ]
        })
        .collect();
    (columns, rows)
}

/// `SHOW INDEX INFO` (Memgraph).
pub(crate) fn show_index_info(m: &Manifest) -> Rows {
    let columns = cols(&["index type", "label", "property", "count"]);
    let rows = index_views(m)
        .into_iter()
        .map(|ix| {
            vec![
                s("label+property"),
                s(ix.label),
                s(ix.property),
                PsValue::Null,
            ]
        })
        .collect();
    (columns, rows)
}

/// `SHOW STORAGE INFO` (Memgraph) — key/value storage stats.
pub(crate) fn show_storage_info(m: &Manifest) -> Rows {
    let avg_degree = if m.node_count > 0 {
        m.edge_count as f64 / m.node_count as f64
    } else {
        0.0
    };
    (
        cols(&["storage info", "value"]),
        vec![
            vec![s("vertex_count"), PsValue::Int(m.node_count as i64)],
            vec![s("edge_count"), PsValue::Int(m.edge_count as i64)],
            vec![s("average_degree"), PsValue::Float(avg_degree)],
            vec![s("label_count"), PsValue::Int(m.labels.len() as i64)],
            vec![s("edge_type_count"), PsValue::Int(m.reltypes.len() as i64)],
        ],
    )
}

/// One cache pool's live stats for `SHOW STORAGE INFO`. The server fills one per
/// pool (block / vector / result) from its `CacheMetrics` snapshot + residency.
pub(crate) struct CachePoolStat {
    pub name: &'static str,
    pub bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// `SHOW STORAGE INFO` with the per-pool cache metrics appended. Operators read
/// `<pool>_cache_{bytes,entries,hits,misses,evictions}` to watch residency, hit
/// rate, and eviction pressure — the evidence for tuning the block/vector/result
/// budget split (the three pools are isolated by design, so there is no runtime
/// auto-balancer; these rows are how you balance them by hand).
pub(crate) fn show_storage_info_with_caches(m: &Manifest, pools: &[CachePoolStat]) -> Rows {
    let (cols, mut rows) = show_storage_info(m);
    for p in pools {
        rows.push(vec![
            s(format!("{}_cache_bytes", p.name)),
            PsValue::Int(p.bytes as i64),
        ]);
        rows.push(vec![
            s(format!("{}_cache_entries", p.name)),
            PsValue::Int(p.entries as i64),
        ]);
        rows.push(vec![
            s(format!("{}_cache_hits", p.name)),
            PsValue::Int(p.hits as i64),
        ]);
        rows.push(vec![
            s(format!("{}_cache_misses", p.name)),
            PsValue::Int(p.misses as i64),
        ]);
        rows.push(vec![
            s(format!("{}_cache_evictions", p.name)),
            PsValue::Int(p.evictions as i64),
        ]);
    }
    (cols, rows)
}

/// `CALL db.schema.visualization()` — slater does not materialise a schema graph,
/// so return the well-formed empty shape (Browser shows "no schema" rather than
/// erroring).
pub(crate) fn schema_visualization() -> Rows {
    (
        cols(&["nodes", "relationships"]),
        vec![vec![PsValue::List(vec![]), PsValue::List(vec![])]],
    )
}
