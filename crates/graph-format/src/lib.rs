// SPDX-License-Identifier: Apache-2.0
//! Slater on-disk format — the single owner of the byte layout.
//!
//! Both binaries (`slater-build`, the offline writer, and `slater`, the online
//! reader) depend on this crate so the writer and reader can never drift. Each
//! graph is serialised as an immutable, generation-numbered directory; see
//! [`manifest`] for the inventory and `docs/PLAN.md` for the full design.
//!
//! British English is used throughout docs and messages.

// DESIGN: format version is bumped whenever the byte layout of any `.blk` file,
// the MANIFEST schema, or an index encoding changes incompatibly. The reader
// refuses a generation whose `formatVersion` it does not understand.
/// On-disk format version understood by this build.
///
/// v2 adds the per-reltype endpoint postings (`reltype_src.post` /
/// `reltype_tgt.post`) and their manifest count vectors — see [`postings`].
/// v3 adds the per-(label, property) value→count histograms (`prop_hist.blk`)
/// and their manifest descriptors — see [`histogram`].
pub const FORMAT_VERSION: u32 = 3;

/// The Slater on-disk magic, written at the head of the MANIFEST for a quick
/// "is this a Slater generation at all" check before any JSON parsing.
pub const MAGIC: &[u8; 8] = b"SLATER01";

pub mod blockcache;
pub mod blockfile;
pub mod codec;
pub mod columns;
pub mod consolidate_dump;
pub mod crypto;
pub mod extsort;
pub mod histogram;
pub mod ids;
pub mod integrity;
pub mod isam;
pub mod manifest;
pub mod membudget;
pub mod nodelabels;
pub mod postings;
pub mod pq;
pub mod setmanifest;
pub mod store;
pub mod topology;
pub mod vamana;
pub mod vectors;
pub mod wire;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_version_is_stable() {
        // A change here is a deliberate, breaking format bump — update readers.
        assert_eq!(FORMAT_VERSION, 3);
        assert_eq!(MAGIC, b"SLATER01");
    }
}
