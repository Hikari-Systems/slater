// SPDX-License-Identifier: Apache-2.0
//! Typed representation of a parsed dump statement.
//!
//! The pest parser turns each raw statement string into one of these; the builder
//! consumes a stream of them. Marker/cleanup lines parse to [`Statement::Ignored`]
//! and carry no data. Property values reuse [`graph_format::ids::Value`] directly
//! so a `vecf32([...])` literal is already a first-class [`Value::Vector`] by the
//! time it reaches the builder.

use graph_format::ids::Value;
use serde::{Deserialize, Serialize};

/// Which entity a range index attaches to (mirrors `graph_format`'s `EntityKind`
/// but kept local so the parser layer does not depend on the manifest types).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    /// `MERGE (n:L {k:v}) SET …` / `MATCH (n:L {k:v}) SET …` — overwrite the
    /// properties of node(s) created earlier in the same build (overlay dialect).
    NodeOverwrite(NodeOverwriteStmt),
    /// `MERGE|MATCH (a:L {k:v})-[r:T]->(b:M {j:w}) SET …` — overwrite edge props.
    EdgeOverwrite(EdgeOverwriteStmt),
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

/// One half of an edge-overwrite endpoint match: locate a node by `label` having
/// property `key == value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeMatch {
    pub label: String,
    pub key: String,
    pub value: Value,
}

/// `MERGE|MATCH (n:L {k:v}) SET …`. `is_merge` selects create-on-absent semantics:
/// a MERGE with zero matches creates a node (label `L`, property `k=v`, plus the
/// SET props); a MATCH with zero matches is a no-op (with a stderr warning).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeOverwriteStmt {
    pub match_: NodeMatch,
    pub is_merge: bool,
    pub set_props: Vec<(String, Value)>,
}

/// `MERGE|MATCH (a:L {k:v})-[r:T]->(b:M {j:w}) SET r.… = …`. Endpoints are matched
/// by label+property like [`NodeOverwriteStmt`]; the edge is then located by
/// `(matched src, matched dst, reltype)`. Edge create-on-absent is not supported in
/// v1, so `is_merge` differs from MATCH only in the (currently identical) 0-match
/// error path — retained for forward compatibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeOverwriteStmt {
    pub src: NodeMatch,
    pub dst: NodeMatch,
    pub reltype: String,
    pub is_merge: bool,
    pub set_props: Vec<(String, Value)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeIndexStmt {
    pub entity: Entity,
    pub label_or_type: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexStmt {
    pub label: String,
    pub property: String,
    pub dim: u32,
    /// Raw metric token from the dump (e.g. `"cosine"`); normalised by the builder.
    pub metric: String,
}
