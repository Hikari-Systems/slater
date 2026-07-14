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
/// v4 changes the dense degree column (`node_degrees.blk`) from raw-u32 records in a
/// zstd block container to per-chunk Elias–Fano records in a Raw block container — see
/// [`degree_ef`]. The encoding is self-describing (per-chunk codec tag) but shares no bytes
/// with v3's, so the version bump makes a v3 generation refuse at open (`generation.rs`)
/// rather than misread the column; zero legacy installs, so old generations are rebuilt.
/// v5 collapses the per-edge `edge_id` in the **forward** CSR half (`topology.csr.blk`) to a
/// single `edge_id_base` per record (derived `edge_id = base + k`), since a source's outgoing
/// edge ids are dense-contiguous — see [`topology`]. Reverse records keep the per-edge id.
/// v6 re-encodes the "no-decompress, usable-in-encoded-form" indexes onto the generic plane
/// codec ([`plane`]): the per-reltype endpoint postings (`reltype_src.post`/`reltype_tgt.post`)
/// move from delta-varint-in-zstd to Elias–Fano records in a Raw container — see [`postings`].
/// (Later v6 slices re-plane `node_labels.blk`, `topology.csr.blk` and `isam`.) Batched into one
/// bump so the 91.6M generation rebuilds once; zero legacy installs, so old generations refuse.
/// v7 folds the degenerate `Constant` codec into single-run `Rle` in both the generic plane
/// codec ([`plane`]) and the degree column ([`degree_ef`]) — a constant is just a run, so the
/// dedicated `Constant` tag is gone. (Further v7 slices re-plane `node_labels.blk` and the
/// reltype postings' dense endpoint sets.)
/// v8 reshapes the vector ANN stores (FreshDiskANN S1): the `.vamana` record drops its
/// `node_id` and becomes **pure geometry** (`dim ‖ vec ‖ degree ‖ adj`), so a consolidation
/// — which permutes every dense id — need not rewrite it at all; the `.pq` file becomes the
/// single layout→id map, with [`pq::HOLE`] (`u64::MAX`) marking a tombstoned record; stored
/// vectors are **raw** rather than L2-normalised (magnitudes survive a rebuild); and the
/// Vamana arm serves L2 and dot indexes, not cosine alone — see [`vamana`] and [`pq`].
pub const FORMAT_VERSION: u32 = 8;

/// The Slater on-disk magic, written at the head of the MANIFEST for a quick
/// "is this a Slater generation at all" check before any JSON parsing.
pub const MAGIC: &[u8; 8] = b"SLATER01";

pub mod blockcache;
pub mod blockfile;
pub mod codec;
pub mod columns;
pub mod consolidate_dump;
pub mod crypto;
pub mod degree_ef;
pub mod extents;
pub mod extsort;
pub mod histogram;
pub mod hubdegree;
pub mod ids;
pub mod integrity;
pub mod isam;
pub mod manifest;
pub mod membudget;
pub mod nodedegree;
pub mod nodelabels;
pub mod plane;
pub mod postings;
pub mod pq;
pub mod rwvamana;
pub mod segindex;
pub mod segmanifest;
pub mod segment;
pub mod segpostings;
pub mod segvamana;
pub mod segvectors;
pub mod setmanifest;
pub mod store;
pub mod topology;
pub mod vamana;
pub mod vamana_delete;
pub mod vectors;
pub mod wire;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_version_is_stable() {
        // A change here is a deliberate, breaking format bump — update readers.
        assert_eq!(FORMAT_VERSION, 8);
        assert_eq!(MAGIC, b"SLATER01");
    }
}
