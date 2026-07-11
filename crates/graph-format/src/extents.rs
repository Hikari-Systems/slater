// SPDX-License-Identifier: Apache-2.0
//! The **extent table** — the resident routing structure that maps a dense id to
//! the segment of a generation set that owns it (the segmented-core track; see
//! `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! # Banded ids
//! In a set, the base generation owns id band `[0, base_count)` and every flush
//! appends a fresh, contiguous band `[b, b+k)` to a new upper segment. Existing ids
//! never move, so a node's / edge's dense id alone names its owning segment. The
//! extent table is the sorted, binary-searched index of those bands: for both nodes
//! and edges it is a small `Vec<Extent>` (one entry per segment, oldest→newest,
//! tiling `[0, total)`), so routing an id is one `partition_point` over a resident
//! slice — no block read.
//!
//! A singleton set (base only) has a one-entry table `[(0, base_count) → Base]`, so
//! every id routes to the base and behaviour is identical to the pre-set format.
//!
//! # Invariants (checked at construction)
//! Bands are sorted ascending, non-overlapping, and **tile** `[0, total)` with no
//! gap: the first band starts at 0 and each band's end is the next band's base. This
//! is one of the open-time invariants the plan calls out ("bands tile, routing
//! monotone") — a set whose segment bands do not tile is rejected here, before any
//! read trusts the routing.

use anyhow::{bail, Result};

use crate::setmanifest::SetManifest;

/// Which member of a generation set owns a band. `Base` is the base generation;
/// `Upper(i)` is `set.segments[i]` (0-indexed, oldest→newest). The derived ordering
/// is the newest-wins precedence order: `Base < Upper(0) < Upper(1) < …`, so a later
/// (higher) ordinal shadows an earlier one when the read path folds levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SegmentOrd {
    /// The base generation, owner of band `[0, base_count)`.
    Base,
    /// Upper segment `set.segments[i]`.
    Upper(usize),
}

/// One id band `[base, end)` and the segment that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// Inclusive band start.
    pub base: u64,
    /// Exclusive band end.
    pub end: u64,
    /// The segment that owns every id in `[base, end)`.
    pub owner: SegmentOrd,
}

impl Extent {
    /// Number of ids in this band.
    #[inline]
    pub fn len(&self) -> u64 {
        self.end - self.base
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.end == self.base
    }
}

/// A sorted, tiling routing table over one id space (nodes or edges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentTable {
    /// Bands sorted ascending by `base`, tiling `[0, total)` without gap or overlap.
    bands: Vec<Extent>,
    /// One-past-the-last id (== the total count over the whole set).
    total: u64,
}

impl ExtentTable {
    /// Build a table from bands given oldest→newest (base first). Each `(len, owner)`
    /// contributes a contiguous band appended after the previous one; a zero-length
    /// band is permitted (a segment that added no ids of this kind) but still records
    /// its owner at a zero-width point, which routing never selects.
    ///
    /// Validates that the owners arrive in strictly increasing ordinal order (base,
    /// then upper segments in stack order) so the table is unambiguous.
    pub fn from_lengths(lengths: impl IntoIterator<Item = (u64, SegmentOrd)>) -> Result<Self> {
        let mut bands = Vec::new();
        let mut cursor = 0u64;
        let mut prev_owner: Option<SegmentOrd> = None;
        for (len, owner) in lengths {
            if let Some(p) = prev_owner {
                if owner <= p {
                    bail!("extent owners must strictly increase; {owner:?} follows {p:?}");
                }
            }
            let base = cursor;
            let end = cursor
                .checked_add(len)
                .ok_or_else(|| anyhow::anyhow!("extent band overflows u64 at base {base}"))?;
            bands.push(Extent { base, end, owner });
            cursor = end;
            prev_owner = Some(owner);
        }
        Self::from_bands(bands, cursor)
    }

    /// Build directly from explicit bands, validating the tiling invariant.
    pub fn from_bands(bands: Vec<Extent>, total: u64) -> Result<Self> {
        let t = Self { bands, total };
        t.validate()?;
        Ok(t)
    }

    /// A singleton table: the base owns everything in `[0, base_count)`.
    pub fn singleton(base_count: u64) -> Self {
        Self {
            bands: vec![Extent {
                base: 0,
                end: base_count,
                owner: SegmentOrd::Base,
            }],
            total: base_count,
        }
    }

    /// Total ids across the whole set (one-past-the-last id).
    #[inline]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// The bands, oldest→newest.
    #[inline]
    pub fn bands(&self) -> &[Extent] {
        &self.bands
    }

    /// Route a dense id to its owning segment, or `None` if `id >= total` (no member
    /// owns it). One `partition_point` over the resident band slice — no block read.
    #[inline]
    pub fn route(&self, id: u64) -> Option<SegmentOrd> {
        if id >= self.total {
            return None;
        }
        // Largest band whose base <= id. Bands tile [0, total) with no gap, so the
        // predecessor of the first base strictly greater than `id` owns it.
        let idx = self.bands.partition_point(|e| e.base <= id);
        // idx >= 1 because band[0].base == 0 <= id (id < total, so at least one band).
        debug_assert!(idx >= 1);
        Some(self.bands[idx - 1].owner)
    }

    /// The band owned by `ord`, if `ord` is a member of this set. Zero-width bands are
    /// returned as-is (a segment that contributed no ids of this kind).
    pub fn band_of(&self, ord: SegmentOrd) -> Option<(u64, u64)> {
        self.bands
            .iter()
            .find(|e| e.owner == ord)
            .map(|e| (e.base, e.end))
    }

    fn validate(&self) -> Result<()> {
        let mut cursor = 0u64;
        let mut prev_owner: Option<SegmentOrd> = None;
        for (i, e) in self.bands.iter().enumerate() {
            if e.base != cursor {
                bail!(
                    "extent band {i} starts at {} but the previous band ended at {cursor} \
                     (bands must tile [0, total) with no gap or overlap)",
                    e.base
                );
            }
            if e.end < e.base {
                bail!("extent band {i} has end {} < base {}", e.end, e.base);
            }
            if let Some(p) = prev_owner {
                if e.owner <= p {
                    bail!(
                        "extent owners must strictly increase; band {i} owner {:?} \
                         does not follow {p:?}",
                        e.owner
                    );
                }
            }
            cursor = e.end;
            prev_owner = Some(e.owner);
        }
        if cursor != self.total {
            bail!(
                "extent bands tile up to {cursor} but declared total is {} \
                 (Σ band lengths must equal the total id count)",
                self.total
            );
        }
        Ok(())
    }
}

/// The node and edge routing tables for a whole generation set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extents {
    pub nodes: ExtentTable,
    pub edges: ExtentTable,
}

impl Extents {
    /// A singleton set's routing: the base owns `[0, base_node_count)` /
    /// `[0, base_edge_count)`.
    pub fn singleton(base_node_count: u64, base_edge_count: u64) -> Self {
        Self {
            nodes: ExtentTable::singleton(base_node_count),
            edges: ExtentTable::singleton(base_edge_count),
        }
    }

    /// Build routing from a set manifest plus the base generation's counts. The base
    /// contributes `[0, base_node_count)` / `[0, base_edge_count)`; each upper segment
    /// contributes its declared `node_band` / `edge_band`. The bands are validated to
    /// tile — a set whose segment bands do not append contiguously to the base is
    /// rejected here.
    pub fn from_set(set: &SetManifest, base_node_count: u64, base_edge_count: u64) -> Result<Self> {
        let mut node_bands = vec![Extent {
            base: 0,
            end: base_node_count,
            owner: SegmentOrd::Base,
        }];
        let mut edge_bands = vec![Extent {
            base: 0,
            end: base_edge_count,
            owner: SegmentOrd::Base,
        }];
        for (i, seg) in set.segments.iter().enumerate() {
            let owner = SegmentOrd::Upper(i);
            node_bands.push(Extent {
                base: seg.node_band.0,
                end: seg.node_band.1,
                owner,
            });
            edge_bands.push(Extent {
                base: seg.edge_band.0,
                end: seg.edge_band.1,
                owner,
            });
        }
        let node_total = node_bands.last().map(|e| e.end).unwrap_or(0);
        let edge_total = edge_bands.last().map(|e| e.end).unwrap_or(0);
        Ok(Self {
            nodes: ExtentTable::from_bands(node_bands, node_total)?,
            edges: ExtentTable::from_bands(edge_bands, edge_total)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Generation;
    use crate::setmanifest::{SegmentRef, SetManifest};

    fn uuid(n: u128) -> Generation {
        Generation(uuid::Uuid::from_u128(n))
    }

    #[test]
    fn singleton_routes_everything_to_base() {
        let t = ExtentTable::singleton(100);
        assert_eq!(t.total(), 100);
        assert_eq!(t.route(0), Some(SegmentOrd::Base));
        assert_eq!(t.route(99), Some(SegmentOrd::Base));
        assert_eq!(t.route(100), None);
        assert_eq!(t.route(u64::MAX), None);
    }

    #[test]
    fn empty_singleton_routes_nothing() {
        let t = ExtentTable::singleton(0);
        assert_eq!(t.total(), 0);
        assert_eq!(t.route(0), None);
    }

    #[test]
    fn from_lengths_tiles_and_routes() {
        // base owns [0,10), seg0 owns [10,13), seg1 owns [13,20).
        let t = ExtentTable::from_lengths([
            (10, SegmentOrd::Base),
            (3, SegmentOrd::Upper(0)),
            (7, SegmentOrd::Upper(1)),
        ])
        .unwrap();
        assert_eq!(t.total(), 20);
        assert_eq!(t.route(0), Some(SegmentOrd::Base));
        assert_eq!(t.route(9), Some(SegmentOrd::Base));
        assert_eq!(t.route(10), Some(SegmentOrd::Upper(0)));
        assert_eq!(t.route(12), Some(SegmentOrd::Upper(0)));
        assert_eq!(t.route(13), Some(SegmentOrd::Upper(1)));
        assert_eq!(t.route(19), Some(SegmentOrd::Upper(1)));
        assert_eq!(t.route(20), None);
        assert_eq!(t.band_of(SegmentOrd::Upper(0)), Some((10, 13)));
        assert_eq!(t.band_of(SegmentOrd::Upper(2)), None);
    }

    #[test]
    fn zero_width_band_is_never_routed_to() {
        // seg0 added no nodes: its band is [10,10). Routing must skip it and never
        // return Upper(0) for any id.
        let t = ExtentTable::from_lengths([
            (10, SegmentOrd::Base),
            (0, SegmentOrd::Upper(0)),
            (5, SegmentOrd::Upper(1)),
        ])
        .unwrap();
        assert_eq!(t.total(), 15);
        assert_eq!(t.band_of(SegmentOrd::Upper(0)), Some((10, 10)));
        for id in 0..15 {
            let owner = t.route(id).unwrap();
            assert_ne!(
                owner,
                SegmentOrd::Upper(0),
                "id {id} routed to a zero-width band"
            );
        }
        assert_eq!(t.route(10), Some(SegmentOrd::Upper(1)));
    }

    #[test]
    fn ordinal_precedence_is_newest_wins() {
        // Derived Ord: Base < Upper(0) < Upper(1). The read path relies on this so a
        // higher ordinal shadows a lower one.
        assert!(SegmentOrd::Base < SegmentOrd::Upper(0));
        assert!(SegmentOrd::Upper(0) < SegmentOrd::Upper(1));
    }

    #[test]
    fn rejects_non_tiling_bands() {
        // A gap between base end (10) and the segment base (12).
        let bad = ExtentTable::from_bands(
            vec![
                Extent {
                    base: 0,
                    end: 10,
                    owner: SegmentOrd::Base,
                },
                Extent {
                    base: 12,
                    end: 15,
                    owner: SegmentOrd::Upper(0),
                },
            ],
            15,
        );
        let err = bad.unwrap_err();
        assert!(format!("{err:#}").contains("tile"), "{err:#}");
    }

    #[test]
    fn rejects_wrong_total() {
        let bad = ExtentTable::from_bands(
            vec![Extent {
                base: 0,
                end: 10,
                owner: SegmentOrd::Base,
            }],
            11,
        );
        let err = bad.unwrap_err();
        assert!(format!("{err:#}").contains("total"), "{err:#}");
    }

    #[test]
    fn rejects_out_of_order_owners() {
        let bad =
            ExtentTable::from_lengths([(10, SegmentOrd::Upper(1)), (5, SegmentOrd::Upper(0))]);
        let err = bad.unwrap_err();
        assert!(format!("{err:#}").contains("increase"), "{err:#}");
    }

    #[test]
    fn extents_from_singleton_set() {
        let set = SetManifest::singleton(uuid(1), 0);
        let e = Extents::from_set(&set, 50, 200).unwrap();
        assert_eq!(e.nodes.route(49), Some(SegmentOrd::Base));
        assert_eq!(e.nodes.route(50), None);
        assert_eq!(e.edges.route(199), Some(SegmentOrd::Base));
        assert_eq!(e.edges.route(200), None);
    }

    #[test]
    fn extents_from_set_with_segments() {
        let mut set = SetManifest::singleton(uuid(1), 0);
        set.segments.push(SegmentRef {
            uuid: uuid(2),
            node_band: (50, 60),
            edge_band: (200, 205),
            content_hash: String::new(),
        });
        set.segments.push(SegmentRef {
            uuid: uuid(3),
            node_band: (60, 65),
            edge_band: (205, 205), // added no edges
            content_hash: String::new(),
        });
        let e = Extents::from_set(&set, 50, 200).unwrap();
        assert_eq!(e.nodes.total(), 65);
        assert_eq!(e.nodes.route(55), Some(SegmentOrd::Upper(0)));
        assert_eq!(e.nodes.route(64), Some(SegmentOrd::Upper(1)));
        assert_eq!(e.edges.total(), 205);
        assert_eq!(e.edges.route(204), Some(SegmentOrd::Upper(0)));
        // Upper(1) added no edges — no edge id routes to it.
        for id in 0..205 {
            assert_ne!(e.edges.route(id), Some(SegmentOrd::Upper(1)));
        }
    }

    #[test]
    fn extents_from_set_rejects_discontiguous_segment_band() {
        let mut set = SetManifest::singleton(uuid(1), 0);
        set.segments.push(SegmentRef {
            uuid: uuid(2),
            node_band: (55, 60), // gap: base ends at 50, segment starts at 55
            edge_band: (200, 205),
            content_hash: String::new(),
        });
        let err = Extents::from_set(&set, 50, 200).unwrap_err();
        assert!(format!("{err:#}").contains("tile"), "{err:#}");
    }
}
