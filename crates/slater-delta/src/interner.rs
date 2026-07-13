// SPDX-License-Identifier: Apache-2.0
//! Delta-local symbol interner.
//!
//! Labels, property keys and relationship types that appear in delta writes need
//! stable ids for the lifetime of the delta. Some already exist in the core's
//! manifest; some are new to the delta. Rather than couple the runtime write path
//! to the core's global symbol table, the delta keeps its **own** first-seen
//! interner (a mirror of the builder's `shared::Interner`) and reconciles to the
//! core/global ids at consolidation — for the dump-and-rebuild consolidation path
//! this reconciliation happens implicitly through the text round-trip.
//!
//! First-seen assignment (`id = names.len()`) keeps the mapping deterministic and
//! round-trippable through a checkpoint.

use std::collections::HashMap;
use std::sync::Arc;

use crate::identity::SymbolId;

/// First-seen interner: name -> dense id, preserving insertion order.
///
/// Each first-seen name is heap-allocated **once** as an `Arc<str>` shared between the
/// id table (`names`) and the lookup map (`map`); the map holds a refcounted handle to
/// the same allocation rather than a second copy of the string. `Arc` (not `Rc`) so the
/// interner — and the [`crate::memtable::Memtable`] that owns it — stays `Send + Sync`
/// for the published read snapshot. `HashMap<Arc<str>, _>: Borrow<str>` keeps lookups
/// by `&str` allocation-free.
#[derive(Debug, Default, Clone)]
pub struct Interner {
    map: HashMap<Arc<str>, SymbolId>,
    names: Vec<Arc<str>>,
}

impl Interner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert-or-get: returns the existing id, or assigns the next one first-seen.
    pub fn intern(&mut self, name: &str) -> SymbolId {
        if let Some(&id) = self.map.get(name) {
            return id;
        }
        let id = self.names.len() as SymbolId;
        // One allocation, shared: the map key is a refcount bump, not a second copy.
        let shared: Arc<str> = Arc::from(name);
        self.names.push(Arc::clone(&shared));
        self.map.insert(shared, id);
        id
    }

    /// Look up without inserting.
    pub fn get(&self, name: &str) -> Option<SymbolId> {
        self.map.get(name).copied()
    }

    /// Resolve an id back to its name.
    pub fn name(&self, id: SymbolId) -> Option<&str> {
        self.names.get(id as usize).map(|s| &**s)
    }

    /// The ordered name table (id == index).
    pub fn names(&self) -> &[Arc<str>] {
        &self.names
    }

    /// Rebuild from a persisted name table (for resume / checkpoint restore).
    pub fn from_names(names: Vec<String>) -> Self {
        let names: Vec<Arc<str>> = names.into_iter().map(Arc::from).collect();
        let map = names
            .iter()
            .enumerate()
            .map(|(i, n)| (Arc::clone(n), i as SymbolId))
            .collect();
        Self { map, names }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_seen_assigns_dense_ids_in_order() {
        let mut it = Interner::new();
        assert_eq!(it.intern("Company"), 0);
        assert_eq!(it.intern("ticker"), 1);
        assert_eq!(it.intern("Company"), 0); // idempotent
        assert_eq!(it.intern("Trial"), 2);
        assert_eq!(it.name(1), Some("ticker"));
        assert_eq!(it.get("Trial"), Some(2));
        assert_eq!(it.get("absent"), None);
    }

    #[test]
    fn round_trips_through_names() {
        let mut it = Interner::new();
        it.intern("a");
        it.intern("b");
        let owned: Vec<String> = it.names().iter().map(|n| n.to_string()).collect();
        let restored = Interner::from_names(owned);
        assert_eq!(restored.get("a"), Some(0));
        assert_eq!(restored.get("b"), Some(1));
        assert_eq!(restored.names(), it.names());
    }

    #[test]
    fn first_seen_name_is_allocated_once_and_shared() {
        // The single heap allocation is shared between the id table and the lookup map:
        // exactly two strong refs, both pointing at the same allocation (no second copy).
        let mut it = Interner::new();
        it.intern("Company");
        let handle = &it.names()[0];
        assert_eq!(std::sync::Arc::strong_count(handle), 2);
    }
}
