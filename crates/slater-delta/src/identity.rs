// SPDX-License-Identifier: Apache-2.0
//! Business-key identity for delta records.
//!
//! The core keys topology, ISAM and vector addressing on per-generation **dense
//! ids** which the `cluster` phase permutes on every build — so they are unusable
//! as a stable cross-generation handle. The stable identity, the one the delta
//! layer binds to, is the *business key*:
//!
//! - a node is `(label, key-property, value)` — e.g. `Company.ticker = 'A'`;
//! - an edge is `(src-key, reltype, dst-key)`, optionally narrowed by an
//!   **identifying edge property** — e.g. `-[:MENTIONS {uuid: 'm-1'}]->`.
//!
//! The `label`, `key` and `reltype` fields are **delta-local interned ids**
//! ([`crate::interner`]), stable for the delta's lifetime and reconciled against
//! the core's global symbol table at consolidation. The `value` is compared
//! *type-exactly* (`{id: 1}` is a different node from `{id: 1.0}`), mirroring the
//! builder's `value_cmp_exact` — so the canonical encoding uses
//! [`graph_format::wire::write_value`], whose byte image already distinguishes
//! `Int(1)` from `Float(1.0)`.

use graph_format::ids::Value;
use graph_format::wire::write_value;

/// A delta-local interned symbol id (label, property key, or reltype).
///
/// Distinct from the core manifest's global ids; reconciled at consolidation.
pub type SymbolId = u32;

/// Business identity of a node: `(label, key-property, value)`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeIdentity {
    /// Delta-local interned id of the node's label.
    pub label: SymbolId,
    /// Delta-local interned id of the identity (business-key) property.
    pub key: SymbolId,
    /// The business-key value, compared type-exactly.
    pub value: Value,
}

/// Business identity of an edge: `(src-key, reltype, dst-key)`, optionally narrowed
/// by an identifying **edge property**.
///
/// Without `key`, one relationship type between a given node pair is one edge — a
/// re-`MERGE` finds it. With `key`, the identity is the property: `MERGE
/// (a)-[e:RELATES_TO {uuid: 'r-1'}]->(b)` and `… {uuid: 'r-2'} …` are two distinct
/// edges between the same pair, which is what lets a store that keys its edges by an
/// opaque id (Graphiti's `uuid`) hold several facts about the same pair without them
/// silently collapsing into one.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeIdentity {
    pub src: NodeIdentity,
    /// Delta-local interned id of the relationship type.
    pub reltype: SymbolId,
    pub dst: NodeIdentity,
    /// The identifying edge property `(key-property, value)`, when the write spelled
    /// one inline. `None` ⇒ the endpoints and type alone identify the edge.
    pub key: Option<(SymbolId, Value)>,
}

impl NodeIdentity {
    pub fn new(label: SymbolId, key: SymbolId, value: Value) -> Self {
        Self { label, key, value }
    }

    /// Canonical, type-exact byte encoding, used as a map key.
    ///
    /// Two identities encode to the same bytes iff they are the same node. The
    /// leading symbol ids disambiguate `(label, key)` before the value, so a
    /// value that happens to encode to a prefix of another can never collide.
    pub fn canonical_key(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        self.encode_into(&mut buf);
        buf
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        graph_format::wire::write_uvarint(buf, self.label as u64);
        graph_format::wire::write_uvarint(buf, self.key as u64);
        write_value(buf, &self.value);
    }
}

impl EdgeIdentity {
    /// An edge identified by its endpoints and type alone.
    pub fn new(src: NodeIdentity, reltype: SymbolId, dst: NodeIdentity) -> Self {
        Self {
            src,
            reltype,
            dst,
            key: None,
        }
    }

    /// An edge identified by its endpoints, type **and** an identifying edge property.
    pub fn keyed(
        src: NodeIdentity,
        reltype: SymbolId,
        dst: NodeIdentity,
        key: Option<(SymbolId, Value)>,
    ) -> Self {
        Self {
            src,
            reltype,
            dst,
            key,
        }
    }

    /// Canonical, type-exact byte encoding of `(src, reltype, dst[, key])`.
    ///
    /// The identifying property is **appended**, and only when present, so a keyless
    /// edge encodes to exactly the bytes it always did. A keyed encoding is strictly
    /// longer than the keyless one it extends, so the two can never collide, and two
    /// keyed edges differ wherever their `(key, value)` differs — `write_value` is
    /// self-delimiting and type-exact.
    pub fn canonical_key(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        self.src.encode_into(&mut buf);
        graph_format::wire::write_uvarint(&mut buf, self.reltype as u64);
        self.dst.encode_into(&mut buf);
        if let Some((key, value)) = &self.key {
            graph_format::wire::write_uvarint(&mut buf, *key as u64);
            write_value(&mut buf, value);
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_exact_values_do_not_collide() {
        // `{id: 1}` (Int) and `{id: 1.0}` (Float) are distinct node identities,
        // matching the builder's type-exact business-key equality.
        let a = NodeIdentity::new(0, 0, Value::Int(1));
        let b = NodeIdentity::new(0, 0, Value::Float(1.0));
        assert_ne!(a.canonical_key(), b.canonical_key());
    }

    #[test]
    fn same_identity_encodes_stably() {
        let a = NodeIdentity::new(3, 7, Value::Str("A".into()));
        let b = NodeIdentity::new(3, 7, Value::Str("A".into()));
        assert_eq!(a.canonical_key(), b.canonical_key());
    }

    #[test]
    fn distinct_label_or_key_separates_identity() {
        let base = NodeIdentity::new(1, 1, Value::Int(5));
        let other_label = NodeIdentity::new(2, 1, Value::Int(5));
        let other_key = NodeIdentity::new(1, 2, Value::Int(5));
        assert_ne!(base.canonical_key(), other_label.canonical_key());
        assert_ne!(base.canonical_key(), other_key.canonical_key());
    }

    #[test]
    fn edge_identity_round_trips_endpoints() {
        let src = NodeIdentity::new(0, 0, Value::Str("a".into()));
        let dst = NodeIdentity::new(0, 0, Value::Str("b".into()));
        let e = EdgeIdentity::new(src.clone(), 4, dst.clone());
        let swapped = EdgeIdentity::new(dst, 4, src);
        // Direction matters: (a)->(b) is not (b)->(a).
        assert_ne!(e.canonical_key(), swapped.canonical_key());
    }

    #[test]
    fn an_identifying_property_separates_same_pair_edges() {
        // The collapse this exists to prevent: two `RELATES_TO` edges between the same
        // pair, distinguished only by their `uuid`, must be two identities.
        let src = NodeIdentity::new(0, 0, Value::Str("a".into()));
        let dst = NodeIdentity::new(0, 0, Value::Str("b".into()));
        let mk = |uuid: &str| {
            EdgeIdentity::keyed(
                src.clone(),
                4,
                dst.clone(),
                Some((9, Value::Str(uuid.into()))),
            )
        };
        assert_ne!(mk("r-1").canonical_key(), mk("r-2").canonical_key());
        assert_eq!(mk("r-1").canonical_key(), mk("r-1").canonical_key());
    }

    #[test]
    fn a_keyless_edge_encodes_exactly_as_before() {
        // The keyless encoding is unchanged (the key is appended, never interleaved), and
        // a keyed edge can never collide with the keyless one it extends.
        let src = NodeIdentity::new(0, 0, Value::Str("a".into()));
        let dst = NodeIdentity::new(0, 0, Value::Str("b".into()));
        let keyless = EdgeIdentity::new(src.clone(), 4, dst.clone());
        let keyed = EdgeIdentity::keyed(src, 4, dst, Some((0, Value::Int(0))));
        let (a, b) = (keyless.canonical_key(), keyed.canonical_key());
        assert!(b.starts_with(&a), "the key is appended, not interleaved");
        assert_ne!(a, b);
    }

    #[test]
    fn a_key_property_name_is_part_of_the_identity() {
        let src = NodeIdentity::new(0, 0, Value::Str("a".into()));
        let dst = NodeIdentity::new(0, 0, Value::Str("b".into()));
        let mk = |k: SymbolId| {
            EdgeIdentity::keyed(src.clone(), 4, dst.clone(), Some((k, Value::Int(1))))
        };
        assert_ne!(mk(1).canonical_key(), mk(2).canonical_key());
    }
}
