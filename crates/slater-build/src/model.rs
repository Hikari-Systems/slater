//! Typed representation of a parsed dump statement.
//!
//! The pest parser turns each raw statement string into one of these; the builder
//! consumes a stream of them. Marker/cleanup lines parse to [`Statement::Ignored`]
//! and carry no data. Property values reuse [`graph_format::ids::Value`] directly
//! so a `vecf32([...])` literal is already a first-class [`Value::Vector`] by the
//! time it reaches the builder.

use graph_format::ids::Value;

/// Which entity a range index attaches to (mirrors `graph_format`'s `EntityKind`
/// but kept local so the parser layer does not depend on the manifest types).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entity {
    Node,
    Edge,
}

/// One parsed dump statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `CREATE (:L1:L2 {..})`. Labels and properties are raw — the builder is
    /// responsible for dropping the `__DumpVertex__` marker label and consuming
    /// the `__dump_id__` property.
    Node(NodeStmt),
    /// `MATCH (a {__dump_id__: i}), (b {__dump_id__: j}) CREATE (a)-[:T {..}]->(b)`.
    Edge(EdgeStmt),
    /// `CREATE INDEX FOR (n:Label) ON (n.prop)` / the `()-[r:T]->()` edge form.
    RangeIndex(RangeIndexStmt),
    /// A vector index declaration (either the `CALL …createNodeIndex` form or the
    /// `createNodeVectorIndex(..)` helper form).
    VectorIndex(VectorIndexStmt),
    /// A marker-setup, cleanup or drop line with nothing to persist.
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeStmt {
    pub labels: Vec<String>,
    pub props: Vec<(String, Value)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgeStmt {
    pub src_dump_id: i64,
    pub dst_dump_id: i64,
    pub reltype: String,
    pub props: Vec<(String, Value)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeIndexStmt {
    pub entity: Entity,
    pub label_or_type: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndexStmt {
    pub label: String,
    pub property: String,
    pub dim: u32,
    /// Raw metric token from the dump (e.g. `"cosine"`); normalised by the builder.
    pub metric: String,
}
