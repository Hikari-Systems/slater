// SPDX-License-Identifier: Apache-2.0
//! Core identifier newtypes and the property-cell value type.
//!
//! Dense internal ids are assigned by the builder: nodes get `0..N`, edges get
//! `0..M`. They are stable only within a single generation (generations are
//! immutable), which is what lets the Vamana layer use block-relative addressing.

use serde::{Deserialize, Serialize};

macro_rules! dense_id {
    ($(#[$m:meta])* $name:ident, $inner:ty) => {
        $(#[$m])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $inner);

        impl $name {
            #[inline]
            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }

        impl From<$inner> for $name {
            #[inline]
            fn from(v: $inner) -> Self {
                Self(v)
            }
        }
    };
}

dense_id!(
    /// Dense node identifier, `0..node_count` within a generation.
    NodeId,
    u64
);
dense_id!(
    /// Dense edge identifier, `0..edge_count` within a generation.
    EdgeId,
    u64
);
dense_id!(
    /// Block identifier within a single `.blk` file.
    BlockId,
    u32
);

/// A graph generation, identified by a UUID and pinned by the `current` pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(pub uuid::Uuid);

impl std::fmt::Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A property-cell value, covering every type the dump grammar can carry.
///
// DESIGN: `Vector` is a first-class dense-`f32` type, distinct from a generic
// `List` of floats, so it can be routed to the vector store and round-tripped to
// the similarity index with its dimensionality preserved. Homogeneous arrays of
// scalars are represented with `List`; the builder rejects ragged arrays.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<Value>),
    Vector(Vec<f32>),
}

impl Value {
    /// Total ordering used by range indexes (ISAM) and `ORDER BY`.
    ///
    /// Values are ranked by type (Null < Bool < Number < Str < List < Vector);
    /// within numbers, `Int` and `Float` compare numerically as `f64` so a
    /// numeric range index behaves the same whether the dump wrote `1` or `1.0`.
    /// `Float` uses `total_cmp`, so `NaN` sorts deterministically.
    pub fn cmp_key(&self, other: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use Value::*;
        fn rank(v: &Value) -> u8 {
            match v {
                Null => 0,
                Bool(_) => 1,
                Int(_) | Float(_) => 2,
                Str(_) => 3,
                List(_) => 4,
                Vector(_) => 5,
            }
        }
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Bool(a), Bool(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.total_cmp(b),
            (Int(a), Float(b)) => (*a as f64).total_cmp(b),
            (Float(a), Int(b)) => a.total_cmp(&(*b as f64)),
            (Str(a), Str(b)) => a.cmp(b),
            (List(a), List(b)) => a
                .iter()
                .zip(b)
                .map(|(x, y)| x.cmp_key(y))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (Vector(a), Vector(b)) => a
                .iter()
                .zip(b)
                .map(|(x, y)| x.total_cmp(y))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (a, b) => rank(a).cmp(&rank(b)),
        }
    }

    /// Human-readable type tag, used in error messages and index catalogues.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Vector(_) => "vector",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_ids_index_and_roundtrip() {
        assert_eq!(NodeId(7).index(), 7);
        assert_eq!(EdgeId::from(3u64), EdgeId(3));
        let j = serde_json::to_string(&NodeId(42)).unwrap();
        assert_eq!(j, "42"); // transparent
        assert_eq!(serde_json::from_str::<NodeId>(&j).unwrap(), NodeId(42));
    }

    #[test]
    fn value_type_names() {
        assert_eq!(Value::Null.type_name(), "null");
        assert_eq!(Value::Vector(vec![0.1, 0.2]).type_name(), "vector");
        // A vector is not the same as a list of floats.
        assert_ne!(
            Value::Vector(vec![1.0]),
            Value::List(vec![Value::Float(1.0)])
        );
    }
}
