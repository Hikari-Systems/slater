// SPDX-License-Identifier: Apache-2.0
//! Resolve a dump's arbitrary `__dump_id__` integers to dense provisional node
//! ids under bounded memory.
//!
//! Edges in the dump reference their endpoints by `__dump_id__` (an arbitrary
//! `i64`, e.g. wikidata starts at 368640), so the builder needs a `dump_id →
//! provisional_node_id` map. A naive `HashMap` would cost 3–6 GB at 91.6M nodes;
//! instead we use a **dense `Vec<u32>`** keyed by `dump_id - min`,
//! which is ~366 MB at 91.6M nodes and independent of the edge count. When the
//! dump ids are too sparse for a dense table to fit the budget we would fall back
//! to an external merge-join (not yet implemented — the target datasets are dense
//! after an offset, so dense always applies).

use anyhow::{bail, Result};

/// Sentinel for "no node assigned to this dump-id slot".
const NONE: u32 = u32::MAX;

/// Sentinel `dump_id` meaning "this node carried no `__dump_id__`" (so it is
/// unreferenceable by any edge). Skipped when building the dense table.
pub const NO_DUMP: i64 = i64::MIN;

/// Maps `__dump_id__` → provisional node id.
#[derive(Debug)]
pub enum DumpResolver {
    /// Dense table: `table[dump_id - min] = prov_node_id` (or [`NONE`]).
    Dense { min: i64, table: Vec<u32> },
}

impl DumpResolver {
    /// Build a dense resolver from `dump_ids[prov] = dump_id` (provisional id =
    /// index). `max_table_bytes` caps the table allocation; if the id span needs
    /// more, the build aborts rather than over-allocating (honouring the
    /// never-OOM invariant).
    pub fn build_dense(dump_ids: &[i64], max_table_bytes: usize) -> Result<Self> {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        let mut any = false;
        for &d in dump_ids {
            if d == NO_DUMP {
                continue;
            }
            min = min.min(d);
            max = max.max(d);
            any = true;
        }
        if !any {
            return Ok(DumpResolver::Dense {
                min: 0,
                table: Vec::new(),
            });
        }
        let span = (max as i128 - min as i128) as u128 + 1;
        let bytes = span.saturating_mul(4);
        if bytes > max_table_bytes as u128 {
            bail!(
                "dump ids span {span} slots ({} MiB dense table) which exceeds the \
                 build memory budget; sparse-id spill resolver is not yet implemented",
                bytes / (1024 * 1024)
            );
        }
        if dump_ids.len() as u128 > u32::MAX as u128 {
            bail!(
                "node count {} exceeds the {} the dense resolver can address",
                dump_ids.len(),
                u32::MAX
            );
        }
        let mut table = vec![NONE; span as usize];
        for (prov, &d) in dump_ids.iter().enumerate() {
            if d == NO_DUMP {
                continue;
            }
            let idx = (d - min) as usize;
            if table[idx] != NONE {
                bail!("duplicate __dump_id__ {d}");
            }
            table[idx] = prov as u32;
        }
        Ok(DumpResolver::Dense { min, table })
    }

    /// Resolve a `__dump_id__` to its provisional node id, or `None` if unknown.
    pub fn get(&self, dump_id: i64) -> Option<u64> {
        match self {
            DumpResolver::Dense { min, table } => {
                if dump_id < *min {
                    return None;
                }
                let idx = (dump_id - *min) as usize;
                match table.get(idx) {
                    Some(&v) if v != NONE => Some(v as u64),
                    _ => None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_resolves_offset_contiguous_ids() {
        // The wikidata shape: ids contiguous but starting at a large offset.
        let base = 368_640i64;
        let dump_ids: Vec<i64> = (0..1000).map(|i| base + i).collect();
        let r = DumpResolver::build_dense(&dump_ids, 1 << 30).unwrap();
        for (prov, &d) in dump_ids.iter().enumerate() {
            assert_eq!(r.get(d), Some(prov as u64));
        }
        assert_eq!(r.get(base - 1), None);
        assert_eq!(r.get(base + 1000), None);
    }

    #[test]
    fn dense_resolves_sparse_but_in_budget() {
        // Non-contiguous ids still resolve as long as the span fits the budget.
        let dump_ids = vec![10i64, 25, 4, 1000, 7];
        let r = DumpResolver::build_dense(&dump_ids, 1 << 20).unwrap();
        assert_eq!(r.get(10), Some(0));
        assert_eq!(r.get(25), Some(1));
        assert_eq!(r.get(4), Some(2));
        assert_eq!(r.get(1000), Some(3));
        assert_eq!(r.get(7), Some(4));
        assert_eq!(r.get(11), None);
    }

    #[test]
    fn rejects_duplicate_dump_id() {
        let dump_ids = vec![5i64, 9, 5];
        assert!(DumpResolver::build_dense(&dump_ids, 1 << 20).is_err());
    }

    #[test]
    fn aborts_when_span_exceeds_budget() {
        // A huge span with few ids would need a giant table — refuse rather than OOM.
        let dump_ids = vec![0i64, 1_000_000_000];
        let err = DumpResolver::build_dense(&dump_ids, 1 << 20).unwrap_err();
        assert!(err.to_string().contains("exceeds the build memory budget"));
    }

    #[test]
    fn empty_is_ok() {
        let r = DumpResolver::build_dense(&[], 1 << 20).unwrap();
        assert_eq!(r.get(0), None);
    }
}
