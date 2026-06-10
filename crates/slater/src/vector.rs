// SPDX-License-Identifier: Apache-2.0
//! Brute-force vector KNN.
//!
//! Slater's whole live estate is below the 50k-vector ANN threshold (PLAN.md
//! "Scale & graph inventory"), so the *real* read path for `db.idx.vector`
//! `.queryNodes` is a brute-force scan over the index's full-precision vectors:
//! read the contiguous index group from `vectors.f32.blk` (D10) through the block
//! LRU, score every candidate against the query vector, keep the `k` best. The
//! disk-native Vamana/PQ path (`AnnMode::Vamana`) is M7 — this module is the
//! `AnnMode::BruteForce` arm only.
//!
//! The scoring + selection here is a pure function over a slice of
//! [`VectorEntry`]s so it can be unit-tested against a hand-computed reference
//! independently of the store/cache plumbing; [`crate::exec`] supplies the entries
//! (read through the cache) and the query vector.
//
// DESIGN (D26): `score` mirrors FalkorDB's `db.idx.vector.queryNodes` contract —
// it is the **distance** under the index's metric, and results are ordered
// **ascending** (nearest first), so a smaller score is a closer match. For a
// cosine index that distance is `1 - cosine_similarity`, in `[0, 2]`. The
// companion scalar `similarity(a, b)` returns the complementary cosine
// *similarity* in `[-1, 1]` (so `score == 1 - similarity(query, node)`). Ties on
// score are broken by ascending node id so a query is deterministic.
#![allow(dead_code)]

use anyhow::{bail, Result};

use graph_format::manifest::Metric;
use graph_format::vectors::VectorEntry;

/// One KNN hit: the dense node id and its distance score (see the module DESIGN
/// note — smaller is closer).
#[derive(Debug, Clone, PartialEq)]
pub struct Neighbour {
    pub node_id: u64,
    pub score: f64,
}

/// Cosine similarity of two equal-length vectors, in `[-1, 1]`. A zero-norm
/// vector has no direction, so its similarity to anything is defined as `0`
/// (rather than `NaN`), which makes it maximally distant under the cosine metric.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (*x as f64, *y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// The distance score for `query` vs `candidate` under `metric` — the value
/// surfaced as `score` and ordered ascending. Public so the Vamana arm uses the
/// identical exact re-rank scoring as the brute-force arm (same `score` contract).
pub fn distance(metric: Metric, query: &[f32], candidate: &[f32]) -> f64 {
    match metric {
        Metric::Cosine => 1.0 - cosine_similarity(query, candidate),
        // Inner-product "distance": larger dot product = more similar = smaller
        // distance, so negate. (Not used by the live estate; cosine is the path.)
        Metric::Dot => -query
            .iter()
            .zip(candidate)
            .map(|(x, y)| *x as f64 * *y as f64)
            .sum::<f64>(),
        // Squared Euclidean — monotonic in the true L2 distance, so it orders
        // identically while avoiding a per-candidate sqrt.
        Metric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(x, y)| {
                let d = *x as f64 - *y as f64;
                d * d
            })
            .sum::<f64>(),
    }
}

/// Brute-force `k`-nearest-neighbour scan over a vector index group.
///
/// Every entry must have the same dimensionality as `query` (the store is
/// self-describing about `dim`, so a mismatch is a hard error rather than a
/// silently-wrong score). Returns the `k` lowest-distance neighbours, ascending
/// by score then by node id; fewer than `k` if the group is smaller.
pub fn brute_force_knn(
    entries: &[VectorEntry],
    query: &[f32],
    k: usize,
    metric: Metric,
) -> Result<Vec<Neighbour>> {
    let mut scored = Vec::with_capacity(entries.len());
    for e in entries {
        if e.vector.len() != query.len() {
            bail!(
                "query vector has dimension {} but indexed node {} has dimension {}",
                query.len(),
                e.node_id,
                e.vector.len()
            );
        }
        scored.push(Neighbour {
            node_id: e.node_id,
            score: distance(metric, query, &e.vector),
        });
    }
    // Ascending by distance, ties broken by node id for a deterministic result.
    scored.sort_by(|a, b| {
        a.score
            .total_cmp(&b.score)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    scored.truncate(k);
    Ok(scored)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(node_id: u64, v: &[f32]) -> VectorEntry {
        VectorEntry {
            node_id,
            vector: v.to_vec(),
        }
    }

    #[test]
    fn cosine_similarity_matches_hand_computation() {
        // Identical direction → 1; orthogonal → 0; opposite → -1.
        assert!((cosine_similarity(&[1.0, 0.0], &[2.0, 0.0]) - 1.0).abs() < 1e-12);
        assert!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-12);
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-12);
        // A worked non-trivial case: a=(1,2,3), b=(2,0,1).
        // dot=2+0+3=5; |a|=sqrt(14); |b|=sqrt(5); cos=5/sqrt(70).
        let want = 5.0 / 70.0f64.sqrt();
        assert!((cosine_similarity(&[1.0, 2.0, 3.0], &[2.0, 0.0, 1.0]) - want).abs() < 1e-12);
    }

    #[test]
    fn zero_norm_vector_is_maximally_distant() {
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
        let n = brute_force_knn(&[entry(7, &[0.0, 0.0])], &[1.0, 1.0], 1, Metric::Cosine).unwrap();
        assert!((n[0].score - 1.0).abs() < 1e-12);
    }

    #[test]
    fn knn_orders_by_distance_with_scores_matching_reference() {
        // Query near node 1's direction; node 2 orthogonal; node 0 opposite-ish.
        let entries = vec![
            entry(0, &[-1.0, 0.0]),
            entry(1, &[1.0, 0.1]),
            entry(2, &[0.0, 1.0]),
            entry(3, &[0.9, 0.05]),
        ];
        let query = [1.0, 0.0];
        let got = brute_force_knn(&entries, &query, 3, Metric::Cosine).unwrap();

        // Reference: distance = 1 - cosine_similarity, ascending, tie-break node id.
        let mut reference: Vec<Neighbour> = entries
            .iter()
            .map(|e| Neighbour {
                node_id: e.node_id,
                score: 1.0 - cosine_similarity(&query, &e.vector),
            })
            .collect();
        reference.sort_by(|a, b| {
            a.score
                .total_cmp(&b.score)
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        reference.truncate(3);

        assert_eq!(
            got.iter().map(|n| n.node_id).collect::<Vec<_>>(),
            reference.iter().map(|n| n.node_id).collect::<Vec<_>>()
        );
        for (g, r) in got.iter().zip(&reference) {
            assert!((g.score - r.score).abs() < 1e-12, "score {g:?} vs {r:?}");
        }
        // Sanity: node 3 (smallest angle to +x) is closest, then node 1; node 2
        // (orthogonal) is third and node 0 (-x) is furthest, so it falls outside k=3.
        assert_eq!(got[0].node_id, 3);
        assert_eq!(got[1].node_id, 1);
        assert_eq!(got[2].node_id, 2);
    }

    #[test]
    fn k_larger_than_group_returns_all() {
        let entries = vec![entry(0, &[1.0, 0.0]), entry(1, &[0.0, 1.0])];
        let got = brute_force_knn(&entries, &[1.0, 0.0], 10, Metric::Cosine).unwrap();
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn dimension_mismatch_is_an_error() {
        let entries = vec![entry(0, &[1.0, 0.0, 0.0])];
        let err = brute_force_knn(&entries, &[1.0, 0.0], 1, Metric::Cosine)
            .err()
            .unwrap();
        assert!(err.to_string().contains("dimension"), "got: {err}");
    }
}
