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

/// The right-hand side of a node `SET n.k = …` assignment. A literal value, a
/// reference to another property of the *same* node (`n.other`, resolved against
/// the node's accumulated state at fold time), a pure scalar function call
/// (`coalesce(n.name, n.canonicalName, 'x')`, `toUpper(n.name)`, …), or an
/// expression combining these with infix operators and `CASE`. Functions and
/// operators are evaluated at build time via [`slater_scalar`]; only `Lit` is
/// permitted on edge SET and in the overlay-patch dialect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetExpr {
    Lit(Value),
    /// `n.<key>` — the variable is dropped (v1 patterns bind a single node).
    Prop(String),
    Func {
        name: String,
        args: Vec<SetExpr>,
    },
    /// Arithmetic / concatenation: `l + r`, `l - r`, … (string-concat when either
    /// side is a string — see [`slater_scalar::eval_binop`]).
    BinOp {
        op: slater_scalar::BinOp,
        l: Box<SetExpr>,
        r: Box<SetExpr>,
    },
    /// Comparison: `l = r`, `l <> r`, `l < r`, … (three-valued; see
    /// [`slater_scalar::eval_compare`]).
    Cmp {
        op: slater_scalar::CmpOp,
        l: Box<SetExpr>,
        r: Box<SetExpr>,
    },
    And(Box<SetExpr>, Box<SetExpr>),
    Or(Box<SetExpr>, Box<SetExpr>),
    Not(Box<SetExpr>),
    /// `CASE [subject] WHEN c THEN v … [ELSE e] END`. With `subject = None` this is
    /// the searched form (each `when` is a boolean condition); with `subject =
    /// Some(s)` it is the simple form (`s = when`). Branches are evaluated lazily —
    /// only the first matching `then`, or `els`, is evaluated.
    Case {
        subject: Option<Box<SetExpr>>,
        whens: Vec<(SetExpr, SetExpr)>,
        els: Option<Box<SetExpr>>,
    },
}

/// One half of an edge-overwrite endpoint match: locate a node by `label` (its identity
/// label) having property `key == value`. A node MERGE may name **additional** labels
/// (`MERGE (n:Ident:Other {k:v})`) — the identity label matches/creates the node and the
/// extra labels are written alongside it. Edge endpoints stay single-label (`extra_labels`
/// empty).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeMatch {
    pub label: String,
    /// Labels beyond the identity `label`, in the order named. Empty for the single-label
    /// case and for edge endpoints.
    #[serde(default)]
    pub extra_labels: Vec<String>,
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
    pub set_props: Vec<(String, SetExpr)>,
}

/// `MERGE|MATCH (a:L {k:v})-[r:T]->(b:M {j:w}) [SET r.… = …]`. Endpoints are matched
/// by label+property like [`NodeOverwriteStmt`]; the edge is then located by
/// `(matched src, matched dst, reltype)`.
///
/// In the overlay dialect (a patch over a base built in the same run), this overwrites
/// an existing edge's properties; edge create-on-absent is not supported there. In a
/// business-key MERGE dump (the default import, see [`crate::merge_build`]) the same
/// statement *creates* the relationship, resolving endpoints by business key. `set_props`
/// may be empty (the bare `MERGE (a)-[r:T]->(b)` form ⇒ a property-less edge).
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
