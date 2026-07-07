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

use crate::identity::SymbolId;

/// First-seen interner: name -> dense id, preserving insertion order.
#[derive(Debug, Default, Clone)]
pub struct Interner {
    map: HashMap<String, SymbolId>,
    names: Vec<String>,
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
        self.names.push(name.to_owned());
        self.map.insert(name.to_owned(), id);
        id
    }

    /// Look up without inserting.
    pub fn get(&self, name: &str) -> Option<SymbolId> {
        self.map.get(name).copied()
    }

    /// Resolve an id back to its name.
    pub fn name(&self, id: SymbolId) -> Option<&str> {
        self.names.get(id as usize).map(String::as_str)
    }

    /// The ordered name table (id == index).
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Rebuild from a persisted name table (for resume / checkpoint restore).
    pub fn from_names(names: Vec<String>) -> Self {
        let map = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as SymbolId))
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
        let restored = Interner::from_names(it.names().to_vec());
        assert_eq!(restored.get("a"), Some(0));
        assert_eq!(restored.get("b"), Some(1));
        assert_eq!(restored.names(), it.names());
    }
}
